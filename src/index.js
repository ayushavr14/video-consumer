import "dotenv/config";
import {
  ReceiveMessageCommand,
  SQSClient,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";

const csv = (value) =>
  (value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

// Initialize S3 Client
const s3Client = new S3Client({ region: "ap-south-1" });

async function getPostIdFromS3(bucket, key) {
  try {
    const headCommand = new HeadObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    const { Metadata } = await s3Client.send(headCommand);
    console.log(Metadata);
    return Metadata?.post_id || null;
  } catch (error) {
    console.error("Error fetching metadata:", error);
    return null;
  }
}

const client = new SQSClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: process.env.SQS_CLIENT_ACCESS_KEY_ID,
    secretAccessKey: process.env.SQS_CLIENT_SECRET_ACCESS_KEY,
  },
});

const ecsClient = new ECSClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: process.env.S3_CLIENT_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_CLIENT_SECRET_ACCESS_KEY,
  },
});

async function init() {
  const command = new ReceiveMessageCommand({
    QueueUrl: process.env.SQS_QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 15,
  });
  while (true) {
    const { Messages } = await client.send(command);
    if (!Messages) {
      console.log("No message found");
      continue;
    }

    try {
      for (const message of Messages) {
        const { MessageId, Body } = message;
        console.log(`Message Received`, { MessageId, Body });

        if (!Body) continue;
        const event = JSON.parse(Body);
        if ("Service" in event && "Event" in message) {
          if (event.Event === "s3:TestEvent") {
            await client.send(
              new DeleteMessageCommand({
                QueueUrl: process.env.SQS_QUEUE_URL,
                ReceiptHandle: message.ReceiptHandle,
              })
            );
            continue;
          }
        }
        for (const record of event.Records) {
          const { s3 } = record;
          const {
            bucket,
            object: { key },
          } = s3;

          const postId = await getPostIdFromS3(bucket.name, key);
          console.log(postId);

          // return;

          const runTaskCommand = new RunTaskCommand({
            taskDefinition: process.env.ECS_TASK_DEFINITION,
            cluster: process.env.ECS_CLUSTER,
            launchType: process.env.ECS_LAUNCH_TYPE,
            networkConfiguration: {
              awsvpcConfiguration: {
                assignPublicIp: process.env.ECS_ASSIGN_PUBLIC_IP,
                securityGroups: csv(process.env.ECS_SECURITY_GROUPS),
                subnets: csv(process.env.ECS_SUBNETS),
              },
            },
            overrides: {
              containerOverrides: [
                {
                  name: process.env.ECS_CONTAINER_NAME,
                  environment: [
                    { name: "BUCKET_NAME", value: bucket.name },
                    { name: "KEY", value: key },
                    { name: "POST_ID", value: postId },
                  ],
                },
              ],
            },
          });
          await ecsClient.send(runTaskCommand);
          await client.send(
            new DeleteMessageCommand({
              QueueUrl: process.env.SQS_QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            })
          );
        }
      }
    } catch (error) {
      console.log(error);
    }
  }
}

init();
