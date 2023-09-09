Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(3))
	{
		PG_RETURN_VOID();
	}

	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(3);
	char *colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	bool shardCountIsStrict = false;
	if (distributionColumnText)
	{
		if (PG_ARGISNULL(2))
		{
			PG_RETURN_VOID();
		}

		int shardCount = ShardCount;
		if (!PG_ARGISNULL(4))
		{
			if (!IsColocateWithDefault(colocateWithTableName) &&
				!IsColocateWithNone(colocateWithTableName))
			{
				ereport(ERROR, (errmsg("Cannot use colocate_with with a table "
									   "and shard_count at the same time")));
			}

			shardCount = PG_GETARG_INT32(4);

			/*
			 * If shard_count parameter is given, then we have to
			 * make sure table has that many shards.
			 */
			shardCountIsStrict = true;
		}

		char *distributionColumnName = text_to_cstring(distributionColumnText);
		Assert(distributionColumnName != NULL);

		char distributionMethod = LookupDistributionMethod(distributionMethodOid);

		if (shardCount < 1 || shardCount > MAX_SHARD_COUNT)
		{
			ereport(ERROR, (errmsg("%d is outside the valid range for "
								   "parameter \"shard_count\" (1 .. %d)",
								   shardCount, MAX_SHARD_COUNT)));
		}

		CreateDistributedTable(relationId, distributionColumnName, distributionMethod,
							   shardCount, shardCountIsStrict, colocateWithTableName);
	}
	else
	{
		if (!PG_ARGISNULL(4))
		{
			ereport(ERROR, (errmsg("shard_count can't be specified when the "
								   "distribution column is null because in "
								   "that case it's automatically set to 1")));
		}

		if (!PG_ARGISNULL(2) &&
			LookupDistributionMethod(PG_GETARG_OID(2)) != DISTRIBUTE_BY_HASH)
		{
			/*
			 * As we do for shard_count parameter, we could throw an error if
			 * distribution_type is not NULL when creating a single-shard table.
			 * However, this requires changing the default value of distribution_type
			 * parameter to NULL and this would mean a breaking change for most
			 * users because they're mostly using this API to create sharded
			 * tables. For this reason, here we instead do nothing if the distribution
			 * method is DISTRIBUTE_BY_HASH.
			 */
			ereport(ERROR, (errmsg("distribution_type can't be specified "
								   "when the distribution column is null ")));
		}

		ColocationParam colocationParam = {
			.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
			.colocateWithTableName = colocateWithTableName,
		};
		CreateSingleShardTable(relationId, colocationParam);
	}

	PG_RETURN_VOID();
}
/*
 * CreateSingleShardTable creates a single shard distributed table that
 * doesn't have a shard key.
 */
void
CreateSingleShardTable(Oid relationId, ColocationParam colocationParam)
{
	DistributedTableParams distributedTableParams = {
		.colocationParam = colocationParam,
		.shardCount = 1,
		.shardCountIsStrict = true,
		.distributionColumnName = NULL
	};

	if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * Create the shard of given Citus local table on appropriate node
		 * and drop the local one to convert it into a single-shard distributed
		 * table.
		 */
		ConvertCitusLocalTableToTableType(relationId, SINGLE_SHARD_DISTRIBUTED,
										  &distributedTableParams);
	}
	else
	{
		CreateCitusTable(relationId, SINGLE_SHARD_DISTRIBUTED, &distributedTableParams);
	}
}


