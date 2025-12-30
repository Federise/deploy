import { OpenAPIRoute } from "chanfana";
import { z } from "zod";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  type AppContext,
  AuthorizationHeader,
  NamespaceValue,
  ErrorResponse,
} from "../../types";

const PresignUploadRequest = z.object({
  namespace: NamespaceValue,
  key: z.string(),
  contentType: z.string(),
  size: z.number().int().positive(),
  isPublic: z.boolean().default(false),
});

const PresignUploadResponse = z.object({
  uploadUrl: z.string().url(),
  expiresAt: z.string().datetime(),
});

export class BlobPresignUploadEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "Get a presigned URL for direct upload to R2",
    request: {
      headers: z.object({ authorization: AuthorizationHeader }),
      body: {
        content: { "application/json": { schema: PresignUploadRequest } },
      },
    },
    responses: {
      "200": {
        description: "Presigned URL generated successfully",
        content: { "application/json": { schema: PresignUploadResponse } },
      },
      "400": {
        description: "Bad request",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "503": {
        description: "R2 credentials not configured",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    // Check if R2 credentials are configured
    if (!c.env.R2_ACCOUNT_ID || !c.env.R2_ACCESS_KEY_ID || !c.env.R2_SECRET_ACCESS_KEY) {
      return c.json(
        { code: 503, message: "R2 credentials not configured for presigned URLs" },
        503
      );
    }

    const body = await c.req.json();
    const parsed = PresignUploadRequest.safeParse(body);
    if (!parsed.success) {
      return c.json({ code: 400, message: "Invalid request body" }, 400);
    }

    const { namespace, key, contentType, size, isPublic } = parsed.data;
    const bucketName = isPublic ? "federise-objects-public" : "federise-objects";
    const r2Key = `${namespace}:${key}`;

    // Create S3 client for R2
    const s3Client = new S3Client({
      region: "auto",
      endpoint: `https://${c.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: c.env.R2_ACCESS_KEY_ID,
        secretAccessKey: c.env.R2_SECRET_ACCESS_KEY,
      },
    });

    // Generate presigned PUT URL (valid for 1 hour)
    const expiresIn = 3600;
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: r2Key,
      ContentType: contentType,
      ContentLength: size,
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn });
    const expiresAt = new Date(Date.now() + expiresIn * 1000).toISOString();

    // Pre-create metadata record (will be updated on successful upload confirmation)
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

    return c.json({ uploadUrl, expiresAt });
  }
}
