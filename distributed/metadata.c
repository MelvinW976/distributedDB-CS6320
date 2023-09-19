#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

/* default group size */
int GroupSize = 1;

/* config variable managed via guc.c */
char *CurrentCluster = "default";

/* did current transaction modify pg_dist_node? */
bool TransactionModifiedNodeMetadata = false;

bool EnableMetadataSync = true;

typedef struct NodeMetadata
{
	int32 groupId;
	char *nodeRack;
	bool hasMetadata;
	bool metadataSynced;
	bool isActive;
	Oid nodeRole;
	bool shouldHaveShards;
	char *nodeCluster;
} NodeMetadata;


PG_FUNCTION_INFO_V1(set_coordinator_host);
PG_FUNCTION_INFO_V1(add_node);


/*
 * add_node function adds a new node to the cluster and returns its id. It also
 * replicates all reference tables to the new node.
 */
Datum
add_node(PG_FUNCTION_ARGS)
{

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	nodeMetadata.groupId = PG_GETARG_INT32(2);

	/*
	 * During tests this function is called before nodeRole and nodeCluster have been
	 * created.
	 */
	if (PG_NARGS() == 3)
	{
		nodeMetadata.nodeRole = InvalidOid;
		nodeMetadata.nodeCluster = "default";
	}
	else
	{
		Name nodeClusterName = PG_GETARG_NAME(4);
		nodeMetadata.nodeCluster = NameStr(*nodeClusterName);

		nodeMetadata.nodeRole = PG_GETARG_OID(3);
	}

	if (nodeMetadata.groupId == COORDINATOR_GROUP_ID)
	{
		/* by default, we add the coordinator without shards */
		nodeMetadata.shouldHaveShards = false;
	}


	int nodeId = AddNodeMetadata(nodeNameString, nodePort,
												   &nodeMetadata,
												   &nodeAlreadyExists);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_INT32(nodeId);
}


/*
 * set_coordinator_host configures the hostname and port through which worker
 * nodes can connect to the coordinator.
 */
Datum
set_coordinator_host(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	nodeMetadata.groupId = 0;
	nodeMetadata.shouldHaveShards = false;
	nodeMetadata.nodeRole = PG_GETARG_OID(2);

	Name nodeClusterName = PG_GETARG_NAME(3);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);


	bool isCoordinatorInMetadata = false;
	WorkerNode *coordinatorNode = PrimaryNodeForGroup(COORDINATOR_GROUP_ID,
													  &isCoordinatorInMetadata);
	if (!isCoordinatorInMetadata)
	{
		bool nodeAlreadyExists = false;
		bool localOnly = false;

		/* add the coordinator to pg_dist_node if it was not already added */
		AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
						&nodeAlreadyExists, localOnly);

	}

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}

/*
 * AddNodeMetadata checks the given node information and adds the specified node to the
 * pg_dist_node table of the master and workers with metadata.
 */
static int
AddNodeMetadata(char *nodeName, int32 nodePort, NodeMetadata *nodeMetadata,
				bool *nodeAlreadyExists, bool localOnly)
{
AddNodeMetadata


	/* generate the new node id from the sequence */
	int nextNodeIdInt = GetNextNodeId();

	InsertNodeRow(nextNodeIdInt, nodeName, nodePort, nodeMetadata);

	if (EnableMetadataSync && !localOnly)
	{
		/* send the delete command to all primary nodes with metadata */
		char *nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);
		SendCommandToWorkersWithMetadata(nodeDeleteCommand);

		/* finally prepare the insert command and send it to all primary nodes */
		uint32 primariesWithMetadata = CountPrimariesWithMetadata();
		if (primariesWithMetadata != 0)
		{
			List *workerNodeList = list_make1(workerNode);
			char *nodeInsertCommand = NodeListInsertCommand(workerNodeList);

			SendCommandToWorkersWithMetadata(nodeInsertCommand);
		}
	}

	return workerNode->nodeId;
}

int
GetNextNodeId()
{
	text *sequenceName = cstring_to_text(NODEID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	Datum nextNodeIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	int nextNodeId = DatumGetUInt32(nextNodeIdDatum);

	return nextNodeId;
}


/*
 * InsertNodeRow opens the node system catalog, and inserts a new row with the
 * given values into that system catalog.
 *
 * NOTE: If you call this function you probably need to have taken a
 * ShareRowExclusiveLock then checked that you're not adding a second primary to
 * an existing group. If you don't it's possible for the metadata to become inconsistent.
 */
static void
InsertNodeRow(int nodeid, char *nodeName, int32 nodePort, NodeMetadata *nodeMetadata)
{
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	Datum nodeClusterStringDatum = CStringGetDatum(nodeMetadata->nodeCluster);
	Datum nodeClusterNameDatum = DirectFunctionCall1(namein, nodeClusterStringDatum);

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = UInt32GetDatum(nodeid);
	values[Anum_pg_dist_node_groupid - 1] = Int32GetDatum(nodeMetadata->groupId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_node_nodeport - 1] = UInt32GetDatum(nodePort);
	values[Anum_pg_dist_node_noderack - 1] = CStringGetTextDatum(nodeMetadata->nodeRack);
	values[Anum_pg_dist_node_hasmetadata - 1] = BoolGetDatum(nodeMetadata->hasMetadata);
	values[Anum_pg_dist_node_metadatasynced - 1] = BoolGetDatum(
		nodeMetadata->metadataSynced);
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(nodeMetadata->isActive);
	values[Anum_pg_dist_node_noderole - 1] = ObjectIdGetDatum(nodeMetadata->nodeRole);
	values[Anum_pg_dist_node_nodecluster - 1] = nodeClusterNameDatum;
	values[Anum_pg_dist_node_shouldhaveshards - 1] = BoolGetDatum(
		nodeMetadata->shouldHaveShards);

	Relation pgDistNode = table_open(DistNodeRelationId(), RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistNode, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	/* close relation */
	table_close(pgDistNode, NoLock);
}