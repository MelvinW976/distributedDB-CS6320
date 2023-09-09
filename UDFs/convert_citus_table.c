/*
 * ConvertCitusLocalTableToTableType converts given Citus local table to
 * given table type.
 *
 * This only supports converting Citus local tables to reference tables
 * (by replicating the shard to workers) and single-shard distributed
 * tables (by replicating the shard to the appropriate worker and dropping
 * the local one).
 */
static void
ConvertCitusLocalTableToTableType(Oid relationId, CitusTableType tableType,
								  DistributedTableParams *distributedTableParams)
{
	if (!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("table is not a local table added to metadata")));
	}

	if (tableType != REFERENCE_TABLE && tableType != SINGLE_SHARD_DISTRIBUTED)
	{
		ereport(ERROR, (errmsg("table type is not supported for conversion")));
	}

	if ((tableType == SINGLE_SHARD_DISTRIBUTED) != (distributedTableParams != NULL))
	{
		ereport(ERROR, (errmsg("distributed table params must be provided "
							   "when creating a distributed table and must "
							   "not be otherwise")));
	}

	EnsureCitusTableCanBeCreated(relationId);

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

	LockRelationOid(relationId, ExclusiveLock);

	Var *distributionColumn = NULL;
	CitusTableParams citusTableParams = DecideCitusTableParams(tableType,
															   distributedTableParams);

	uint32 colocationId = INVALID_COLOCATION_ID;
	if (distributedTableParams &&
		distributedTableParams->colocationParam.colocationParamType ==
		COLOCATE_WITH_COLOCATION_ID)
	{
		colocationId = distributedTableParams->colocationParam.colocationId;
	}
	else
	{
		colocationId = ColocationIdForNewTable(relationId, tableType,
											   distributedTableParams,
											   distributionColumn);
	}

	/* check constraints etc. on table based on new distribution params */
	EnsureRelationCanBeDistributed(relationId, distributionColumn,
								   citusTableParams.distributionMethod,
								   colocationId, citusTableParams.replicationModel);

	/*
	 * Regarding the foreign key relationships that given relation is involved,
	 * EnsureRelationCanBeDistributed() only checks the ones where the relation
	 * is the referencing table. And given that the table at hand is a Citus
	 * local table, right now it may only be referenced by a reference table
	 * or a Citus local table. However, given that neither of those two cases
	 * are not applicable for a distributed table, here we throw an error if
	 * that's the case.
	 *
	 * Note that we don't need to check the same if we're creating a reference
	 * table from a Citus local table because all the foreign keys referencing
	 * Citus local tables are supported by reference tables.
	 */
	if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		EnsureNoFKeyFromTableType(relationId, INCLUDE_CITUS_LOCAL_TABLES |
								  INCLUDE_REFERENCE_TABLES);
	}

	EnsureReferenceTablesExistOnAllNodes();

	LockColocationId(colocationId, ShareLock);

	/*
	 * When converting to a single shard table, we want to drop the placement
	 * on the coordinator, but only if transferring to a different node. In that
	 * case, shouldDropLocalPlacement is true. When converting to a reference
	 * table, we always keep the placement on the coordinator, so for reference
	 * tables shouldDropLocalPlacement is always false.
	 */
	bool shouldDropLocalPlacement = false;

	List *targetNodeList = NIL;
	if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		uint32 targetNodeId = SingleShardTableColocationNodeId(colocationId);
		if (targetNodeId != CoordinatorNodeIfAddedAsWorkerOrError()->nodeId)
		{
			bool missingOk = false;
			WorkerNode *targetNode = FindNodeWithNodeId(targetNodeId, missingOk);
			targetNodeList = list_make1(targetNode);

			shouldDropLocalPlacement = true;
		}
	}
	else if (tableType == REFERENCE_TABLE)
	{
		targetNodeList = ActivePrimaryNonCoordinatorNodeList(ShareLock);
		targetNodeList = SortList(targetNodeList, CompareWorkerNodes);
	}

	bool autoConverted = false;
	UpdateNoneDistTableMetadataGlobally(
		relationId, citusTableParams.replicationModel,
		colocationId, autoConverted);

	/* create the shard placement on workers and insert into pg_dist_placement globally */
	if (list_length(targetNodeList) > 0)
	{
		NoneDistTableReplicateCoordinatorPlacement(relationId, targetNodeList);
	}

	if (shouldDropLocalPlacement)
	{
		/*
		 * We don't yet drop the local placement before handling partitions.
		 * Otherewise, local shard placements of the partitions will be gone
		 * before we create them on workers.
		 *
		 * However, we need to delete the related entry from pg_dist_placement
		 * before distributing partitions (if any) because we need a sane metadata
		 * state before doing so.
		 */
		NoneDistTableDeleteCoordinatorPlacement(relationId);
	}

	/* if this table is partitioned table, distribute its partitions too */
	if (PartitionedTable(relationId))
	{
		/* right now we don't allow partitioned reference tables */
		Assert(tableType == SINGLE_SHARD_DISTRIBUTED);

		List *partitionList = PartitionList(relationId);

		char *parentRelationName = generate_qualified_relation_name(relationId);

		/*
		 * When there are many partitions, each call to
		 * ConvertCitusLocalTableToTableType accumulates used memory.
		 * Create and free citus_per_partition_context for each call.
		 */
		MemoryContext citusPartitionContext =
			AllocSetContextCreate(CurrentMemoryContext,
								  "citus_per_partition_context",
								  ALLOCSET_DEFAULT_SIZES);
		MemoryContext oldContext = MemoryContextSwitchTo(citusPartitionContext);

		Oid partitionRelationId = InvalidOid;
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
			ConvertCitusLocalTableToTableType(partitionRelationId, tableType,
											  &childDistributedTableParams);
		}

		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(citusPartitionContext);
	}

	if (shouldDropLocalPlacement)
	{
		NoneDistTableDropCoordinatorPlacementTable(relationId);
	}