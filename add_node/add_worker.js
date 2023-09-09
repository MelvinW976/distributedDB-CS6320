var AWS = require('aws-sdk');
const { Client } = require('pg');

const dbConfig = {
    host: '172.31.40.242',
    database: 'postgres',
    user: 'postgres',
    password: 'Aa82983046?',
    ssl: {
      rejectUnauthorized: false
    }
};

exports.handler = async (event, context) => {
  try {
    const workerIp = event.ip;
    console.log(event);
    // Add the new worker nodes to the Citus cluster
    const client = new Client(dbConfig);
    console.log('hallo');
    await client.connect();
    
    const result = await client.query(`SELECT * from citus_add_node('${workerIp}',5432);`);
    console.log('Successfully add the created worker into the cluster');
    
    const cur_count = await client.query("SELECT * FROM citus_get_active_worker_nodes();");
    
    await client.query("SELECT citus_rebalance_start();");
    console.log('Successfully rebalance the shards');
    await client.end();
    return { statusCode: 200, body: 'Successfully increased a node in your cluster, there are :' + cur_count + 'workers' };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: err.stack };
  }
  
};