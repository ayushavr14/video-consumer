import {
  ReceiveMessageCommand,
  SQSClient,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";

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
            taskDefinition:
              "arn:aws:ecs:ap-south-1:861276104732:task-definition/video-transcoder:4",
            cluster:
              "arn:aws:ecs:ap-south-1:861276104732:cluster/dev-transcoder",
            launchType: "FARGATE",
            networkConfiguration: {
              awsvpcConfiguration: {
                assignPublicIp:"ENABLED",
                securityGroups: ["sg-08be8886ee9e6b28d"],
                subnets: [
                  "subnet-03d2dde7728e7af9e",
                  "subnet-07062d1e2f737f088",
                  "subnet-0d05006a227e9ddf2"
                ],
              },
            },
            overrides: {
              containerOverrides: [
                {
                  name: "video-transcode",
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
