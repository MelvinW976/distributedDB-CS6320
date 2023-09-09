const AWS = require('aws-sdk');

exports.handler = async (event, context) => {
  try {
    const ec2 = new AWS.EC2({ apiVersion: '2016-11-15' });
  
    const params = {
      ImageId: 'ami-088c12daedeedbc9b',
      InstanceType: 't2.micro',
      MinCount: 1,
      MaxCount: 1,
      SubnetId: "subnet-0b971922ee90ce857",
      SecurityGroupIds: ['sg-03f09d1bb5e16a3fb'],
      KeyName: 'citus',
    };
  
    const data = await ec2.runInstances(params).promise();
    console.log(data);
    const instanceId = data.Instances[0].InstanceId;
    const newIp = data.Instances[0].PrivateIpAddress;
    console.log('Created instance', instanceId);
    // Add tags to the instance
    const tagParams = {
      Resources: [instanceId],
      Tags: [
        {
          Key: 'Name',
          Value: 'worker_test',
        },
      ],
    };
    const tagData = await ec2.createTags(tagParams).promise();
    console.log('Instance tagged');
    
    await new Promise((resolve) => setTimeout(resolve, 90000));
    // Invoke the second Lambda function with the private IP as input
    const lambda = new AWS.Lambda({ region: 'us-east-2' });
    
    const invokeParams = {
      FunctionName: 'arn:aws:lambda:us-east-2:299668069227:function:add_worker',
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify({ ip: newIp }),
    };

    const invokeResponse = await lambda.invoke(invokeParams).promise();
    const invokeResult = JSON.parse(invokeResponse.Payload);

    console.log('Second Lambda function invoked:', invokeResult);    

    return { statusCode: 200, body: 'Successfully launched another function'};
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: err.stack };
  }
};