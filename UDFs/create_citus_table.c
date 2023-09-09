/*
 * CreateCitusTable is the internal method that creates a Citus table in
 * given configuration.
 *
 * DistributedTableParams should be non-null only if we're creating a distributed
 * table.
 *
 * This functions contains all necessary logic to create distributed tables. It
 * performs necessary checks to ensure distributing the table is safe. If it is
 * safe to distribute the table, this function creates distributed table metadata,
 * creates shards and copies local data to shards. This function also handles
 * partitioned tables by distributing its partitions as well.
 */
static void
CreateCitusTable(Oid relationId, CitusTableType tableType,
				 DistributedTableParams *distributedTableParams)
{
	if ((tableType == HASH_DISTRIBUTED || tableType == APPEND_DISTRIBUTED ||
		 tableType == RANGE_DISTRIBUTED || tableType == SINGLE_SHARD_DISTRIBUTED) !=
		(distributedTableParams != NULL))
	{
		ereport(ERROR, (errmsg("distributed table params must be provided "
							   "when creating a distributed table and must "
							   "not be otherwise")));
	}

	EnsureCitusTableCanBeCreated(relationId);

	/* allow creating a Citus table on an empty cluster */
	InsertCoordinatorIfClusterEmpty();

	Relation relation = try_relation_open(relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("could not create Citus table: "
							   "relation does not exist")));
	}

	relation_close(relation, NoLock);

	if (tableType == SINGLE_SHARD_DISTRIBUTED && ShardReplicationFactor > 1)
	{
		ereport(ERROR, (errmsg("could not create single shard table: "
							   "citus.shard_replication_factor is greater than 1"),
						errhint("Consider setting citus.shard_replication_factor to 1 "
								"and try again")));
	}

	/*
	 * EnsureTableNotDistributed errors out when relation is a citus table but
	 * we don't want to ask user to first undistribute their citus local tables
	 * when creating distributed tables from them.
	 * For this reason, here we undistribute citus local tables beforehand.
	 * But since UndistributeTable does not support undistributing relations
	 * involved in foreign key relationships, we first drop foreign keys that
	 * given relation is involved, then we undistribute the relation and finally
	 * we re-create dropped foreign keys at the end of this function.
	 */
	List *originalForeignKeyRecreationCommands = NIL;
	if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * We use ConvertCitusLocalTableToTableType instead of CreateCitusTable
		 * to create a reference table or a single-shard table from a Citus
		 * local table.
		 */
		Assert(tableType != REFERENCE_TABLE && tableType != SINGLE_SHARD_DISTRIBUTED);

		/* store foreign key creation commands that relation is involved */
		originalForeignKeyRecreationCommands =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_ALL_TABLE_TYPES);
		relationId = DropFKeysAndUndistributeTable(relationId);
	}
	/*
	 * To support foreign keys between reference tables and local tables,
	 * we drop & re-define foreign keys at the end of this function so
	 * that ALTER TABLE hook does the necessary job, which means converting
	 * local tables to citus local tables to properly support such foreign
	 * keys.
	 */
	else if (tableType == REFERENCE_TABLE &&
			 ShouldEnableLocalReferenceForeignKeys() &&
			 HasForeignKeyWithLocalTable(relationId))
	{
		/*
		 * Store foreign key creation commands for foreign key relationships
		 * that relation has with postgres tables.
		 */
		originalForeignKeyRecreationCommands =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_LOCAL_TABLES);

		/*
		 * Soon we will convert local tables to citus local tables. As
		 * CreateCitusLocalTable needs to use local execution, now we
		 * switch to local execution beforehand so that reference table
		 * creation doesn't use remote execution and we don't error out
		 * in CreateCitusLocalTable.
		 */
		SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

		DropFKeysRelationInvolvedWithTableType(relationId, INCLUDE_LOCAL_TABLES);
	}

	LockRelationOid(relationId, ExclusiveLock);

	EnsureTableNotDistributed(relationId);

	PropagatePrerequisiteObjectsForDistributedTable(relationId);

	Var *distributionColumn = NULL;
	if (distributedTableParams && distributedTableParams->distributionColumnName)
	{
		distributionColumn = BuildDistributionKeyFromColumnName(relationId,
																distributedTableParams->
																distributionColumnName,
																NoLock);
	}

	CitusTableParams citusTableParams = DecideCitusTableParams(tableType,
															   distributedTableParams);

	/*
	 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
	 * our caller already acquired lock on relationId.
	 */
	uint32 colocationId = INVALID_COLOCATION_ID;
	if (distributedTableParams &&
		distributedTableParams->colocationParam.colocationParamType ==
		COLOCATE_WITH_COLOCATION_ID)
	{
		colocationId = distributedTableParams->colocationParam.colocationId;
	}
	else
	{
		/*
		 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
		 * our caller already acquired lock on relationId.
		 */
		colocationId = ColocationIdForNewTable(relationId, tableType,
											   distributedTableParams,
											   distributionColumn);
	}

	EnsureRelationCanBeDistributed(relationId, distributionColumn,
								   citusTableParams.distributionMethod,
								   colocationId, citusTableParams.replicationModel);

	/*
	 * Make sure that existing reference tables have been replicated to all the nodes
	 * such that we can create foreign keys and joins work immediately after creation.
	 *
	 * This will take a lock on the nodes to make sure no nodes are added after we have
	 * verified and ensured the reference tables are copied everywhere.
	 * Although copying reference tables here for anything but creating a new colocation
	 * group, it requires significant refactoring which we don't want to perform now.
	 */
	EnsureReferenceTablesExistOnAllNodes();

	/*
	 * While adding tables to a colocation group we need to make sure no concurrent
	 * mutations happen on the colocation group with regards to its placements. It is
	 * important that we have already copied any reference tables before acquiring this
	 * lock as these are competing operations.
	 */
	LockColocationId(colocationId, ShareLock);

	/* we need to calculate these variables before creating distributed metadata */
	bool localTableEmpty = TableEmpty(relationId);
	Oid colocatedTableId = ColocatedTableId(colocationId);

	/* setting to false since this flag is only valid for citus local tables */
	bool autoConverted = false;

	/* create an entry for distributed table in pg_dist_partition */
	InsertIntoPgDistPartition(relationId, citusTableParams.distributionMethod,
							  distributionColumn,
							  colocationId, citusTableParams.replicationModel,
							  autoConverted);

#if PG_VERSION_NUM >= PG_VERSION_16

	/*
	 * PG16+ supports truncate triggers on foreign tables
	 */
	if (RegularTable(relationId) || IsForeignTable(relationId))
#else

	/* foreign tables do not support TRUNCATE trigger */
	if (RegularTable(relationId))
#endif
	{
		CreateTruncateTrigger(relationId);
	}

	if (tableType == HASH_DISTRIBUTED)
	{
		/* create shards for hash distributed table */
		CreateHashDistributedTableShards(relationId, distributedTableParams->shardCount,
										 colocatedTableId,
										 localTableEmpty);
	}
	else if (tableType == REFERENCE_TABLE)
	{
		/* create shards for reference table */
		CreateReferenceTableShard(relationId);
	}
	else if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		/* create the shard of given single-shard distributed table */
		CreateSingleShardTableShard(relationId, colocatedTableId,
									colocationId);
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		SyncCitusTableMetadata(relationId);
	}

	/*
	 * We've a custom way of foreign key graph invalidation,
	 * see InvalidateForeignKeyGraph().
	 */
	if (TableReferenced(relationId) || TableReferencing(relationId))
	{
		InvalidateForeignKeyGraph();
	}

	/* if this table is partitioned table, distribute its partitions too */
	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);
		Oid partitionRelationId = InvalidOid;
		Oid namespaceId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(namespaceId);
		char *relationName = get_rel_name(relationId);
		char *parentRelationName = quote_qualified_identifier(schemaName, relationName);

		/*
		 * when there are many partitions, each call to CreateDistributedTable
		 * accumulates used memory. Create and free context for each call.
		 */
		MemoryContext citusPartitionContext =
			AllocSetContextCreate(CurrentMemoryContext,
								  "citus_per_partition_context",
								  ALLOCSET_DEFAULT_SIZES);
		MemoryContext oldContext = MemoryContextSwitchTo(citusPartitionContext);

		foreach_oid(partitionRelationId, partitionList)
		{
			MemoryContextReset(citusPartitionContext);

			DistributedTableParams childDistributedTableParams = {
				.colocationParam = {
					.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
					.colocateWithTableName = parentRelationName,
				},
				.shardCount = distributedTableParams->shardCount,
				.shardCountIsStrict = false,
				.distributionColumnName = distributedTableParams->distributionColumnName,
			};
			CreateCitusTable(partitionRelationId, tableType,
							 &childDistributedTableParams);
		}

		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(citusPartitionContext);
	}

	/* copy over data for hash distributed and reference tables */
	if (tableType == HASH_DISTRIBUTED || tableType == SINGLE_SHARD_DISTRIBUTED ||
		tableType == REFERENCE_TABLE)
	{
		if (RegularTable(relationId))
		{
			CopyLocalDataIntoShards(relationId);
		}
	}

	/*
	 * Now recreate foreign keys that we dropped beforehand. As modifications are not
	 * allowed on the relations that are involved in the foreign key relationship,
	 * we can skip the validation of the foreign keys.
	 */
	bool skip_validation = true;
	ExecuteForeignKeyCreateCommandList(originalForeignKeyRecreationCommands,
									   skip_validation);
}