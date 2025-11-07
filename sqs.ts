import { SQSClient } from "@aws-sdk/client-sqs";

const client = new SQSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_CLIENT_ID || "",
    secretAccessKey: process.env.AWS_CLIENT_SECRET || "",
  },
});

export default client;
