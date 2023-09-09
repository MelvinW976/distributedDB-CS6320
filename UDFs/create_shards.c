/*
 * CreateSingleShardTableShardWithRoundRobinPolicy creates a single
 * shard for the given distributedTableId. The created shard does not
 * have min/max values. Unlike CreateReferenceTableShard, the shard is
 * _not_ replicated to all nodes but would have a single placement like
 * Citus local tables.
 *
 * However, this placement doesn't necessarily need to be placed on
 * coordinator. This is determined based on modulo of the colocation
 * id that given table has been associated to.
 */
void
CreateSingleShardTableShardWithRoundRobinPolicy(Oid relationId, uint32 colocationId)
{
	EnsureTableOwner(relationId);

	/* we plan to add shards: get an exclusive lock on relation oid */
	LockRelationOid(relationId, ExclusiveLock);

	/*
	 * Load and sort the worker node list for deterministic placement.
	 *
	 * Also take a RowShareLock on pg_dist_node to disallow concurrent
	 * node list changes that require an exclusive lock.
	 */
	List *workerNodeList = DistributedTablePlacementNodeList(RowShareLock);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	int roundRobinNodeIdx =
		EmptySingleShardTableColocationDecideNodeId(colocationId);

	char shardStorageType = ShardStorageType(relationId);
	text *minHashTokenText = NULL;
	text *maxHashTokenText = NULL;
	uint64 shardId = GetNextShardId();
	InsertShardRow(relationId, shardId, shardStorageType,
				   minHashTokenText, maxHashTokenText);

	int replicationFactor = 1;
	InsertShardPlacementRows(relationId,
							 shardId,
							 workerNodeList,
							 roundRobinNodeIdx,
							 replicationFactor);

	/*
	 * load shard placements for the shard at once after all placement insertions
	 * finished. This prevents MetadataCache from rebuilding unnecessarily after
	 * each placement insertion.
	 */
	List *insertedShardPlacements = ShardPlacementList(shardId);

	/*
	 * We don't need to force using exclusive connections because we're anyway
	 * creating a single shard.
	 */
	bool useExclusiveConnection = false;
	CreateShardsOnWorkers(relationId, insertedShardPlacements,
						  useExclusiveConnection);
}