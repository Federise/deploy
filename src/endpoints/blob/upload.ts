import { z } from "zod";
import { OpenAPIRoute } from "chanfana";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  type AppContext,
  AuthorizationHeader,
  BlobUploadRequest,
  BlobUploadResponse,
  ErrorResponse,
} from "../../types";

export class BlobUploadEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "Initiate blob upload and get presigned URL",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: BlobUploadRequest } },
      },
    },
    responses: {
      "200": {
        description: "Presigned upload URL generated",
        content: { "application/json": { schema: BlobUploadResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const { namespace, key, contentType, size, isPublic } = data.body;

    // Create metadata record in KV
    const metadata = {
      key,
      namespace,
      size,
      contentType,
      uploadedAt: new Date().toISOString(),
      isPublic,
    };

    const kvKey = `__BLOB:${namespace}:${key}`;
    await c.env.KV.put(kvKey, JSON.stringify(metadata));

    // Determine bucket name
    const bucketName = isPublic ? "federise-objects-public" : "federise-objects";
    const r2Key = `${namespace}:${key}`;

    // Configure S3 client for R2
    const s3Client = new S3Client({
      region: "auto",
      endpoint: `https://${c.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: c.env.R2_ACCESS_KEY_ID || "",
        secretAccessKey: c.env.R2_SECRET_ACCESS_KEY || "",
      },
    });

    // Generate presigned URL for upload (PUT method, 1 hour expiry)
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: r2Key,
      ContentType: contentType,
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    const expiresAt = new Date(Date.now() + 3600 * 1000).toISOString();

    return {
      uploadUrl,
      expiresAt,
    };
  }
}
