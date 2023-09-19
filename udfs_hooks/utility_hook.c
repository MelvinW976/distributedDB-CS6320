#include "postgres.h"
#include "commands/utility.h"
#include "miscadmin.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void log_ddl_command(Node *parsetree, const char *queryString);

static void my_ProcessUtility(PlannedStmt *pstmt,
                              const char *queryString,
                              ProcessUtilityContext context,
                              ParamListInfo params,
                              QueryEnvironment *queryEnv,
                              DestReceiver *dest,
                              QueryCompletion *qc);

/*
 * Module initialization function
 */
void _PG_init(void)
{
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = my_ProcessUtility;
}

/*
 * Custom ProcessUtility hook function
 */
static void my_ProcessUtility(PlannedStmt *pstmt,
                              const char *queryString,
                              ProcessUtilityContext context,
                              ParamListInfo params,
                              QueryEnvironment *queryEnv,
                              DestReceiver *dest,
                              QueryCompletion *qc)
{
    /* Call the previous hook, if any */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, qc);

    /* Log DDL commands like CREATE TABLE */
    if (context == PROCESS_UTILITY && pstmt != NULL && IsA(pstmt->utilityStmt, CreateTableStmt))
    {
        log_ddl_command(pstmt->utilityStmt, queryString);
    }
}

/*
 * Log DDL commands
 */
static void log_ddl_command(Node *parsetree, const char *queryString)
{
    /* Extract the DDL command type and table name from the parsetree */
    if (IsA(parsetree, CreateTableStmt))
    {
        CreateTableStmt *createStmt = (CreateTableStmt *)parsetree;
        const char *tableName = createStmt->relation->relname;

        /* Log the DDL command */
        elog(LOG, "DDL command: CREATE TABLE %s", tableName);
    }
}
