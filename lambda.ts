import { LambdaClient } from "@aws-sdk/client-lambda";

const client = new LambdaClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_CLIENT_ID || "",
    secretAccessKey: process.env.AWS_CLIENT_SECRET || "",
  },
});

export default client;
