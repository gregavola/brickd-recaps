import { S3Client } from "@aws-sdk/client-s3";

const client = new S3Client({
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_CLIENT_ID || "",
    secretAccessKey: process.env.AWS_CLIENT_SECRET || "",
  },
});

export default client;
