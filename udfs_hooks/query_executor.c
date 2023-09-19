#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "libpq-fe.h" // Include PostgreSQL's libpq for communication

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(execute_query_on_shard);

// Utility function to execute a query on a specific shard
Datum execute_query_on_shard(PG_FUNCTION_ARGS)
{
    // Extract the query text and shard ID from function arguments
    text *query_text = PG_GETARG_TEXT_PP(0);
    int shard_id = PG_GETARG_INT32(1);

    // Convert the query text to a C string
    char *query_str = text_to_cstring(query_text);

    // Define shard connection parameters (you should configure these)
    const char *shard_host = "shard_db_host";
    const char *shard_port = "shard_db_port";
    const char *shard_dbname = "shard_db_name";
    const char *shard_user = "shard_db_user";
    const char *shard_password = "shard_db_password";

    // Construct the shard connection string
    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo), "host=%s port=%s dbname=%s user=%s password=%s",
             shard_host, shard_port, shard_dbname, shard_user, shard_password);

    // Establish a connection to the shard database
    PGconn *shard_conn = PQconnectdb(conninfo);

    // Check if the connection was successful
    if (PQstatus(shard_conn) != CONNECTION_OK)
    {
        elog(ERROR, "Failed to connect to shard database: %s", PQerrorMessage(shard_conn));
        PQfinish(shard_conn);
        PG_RETURN_NULL();
    }

    // Execute the query on the shard
    PGresult *shard_result = PQexec(shard_conn, query_str);

    // Check for query execution errors
    if (PQresultStatus(shard_result) != PGRES_TUPLES_OK)
    {
        elog(ERROR, "Query execution on shard %d failed: %s", shard_id, PQerrorMessage(shard_conn));
        PQclear(shard_result);
        PQfinish(shard_conn);
        PG_RETURN_NULL();
    }

    // Process the query result here as needed (this is a simplified example)
    int num_rows = PQntuples(shard_result);
    int num_cols = PQnfields(shard_result);

    // Output the query result (this is a simplified example)
    for (int row = 0; row < num_rows; row++)
    {
        for (int col = 0; col < num_cols; col++)
        {
            char *value = PQgetvalue(shard_result, row, col);
            elog(NOTICE, "Shard %d, Row %d, Column %d: %s", shard_id, row, col, value);
        }
    }

    // Clean up and close the shard connection
    PQclear(shard_result);
    PQfinish(shard_conn);

    // Return a result if needed (this is a simplified example)
    PG_RETURN_NULL();
}
