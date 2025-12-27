import { z } from "zod";
import { OpenAPIRoute } from "chanfana";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  type AppContext,
  AuthorizationHeader,
  BlobGetRequest,
  BlobGetResponse,
  ErrorResponse,
} from "../../types";

export class BlobGetEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "Get blob download URL",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: BlobGetRequest } },
      },
    },
    responses: {
      "200": {
        description: "Download URL generated",
        content: { "application/json": { schema: BlobGetResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "404": {
        description: "Blob not found",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const { namespace, key } = data.body;

    // Get metadata from KV
    const kvKey = `__BLOB:${namespace}:${key}`;
    const metadataStr = await c.env.KV.get(kvKey);

    if (!metadataStr) {
      return c.json({ code: 404, message: "Blob not found" }, 404);
    }

    const metadata = JSON.parse(metadataStr);
    const r2Key = `${namespace}:${key}`;

    if (metadata.isPublic) {
      // Return custom domain URL for public blobs (permanent)
      const publicDomain = c.env.PUBLIC_DOMAIN || "https://cdn.example.com";
      const url = `${publicDomain}/${r2Key}`;

      return {
        url,
        metadata,
        // No expiresAt for public URLs
      };
    } else {
      // Generate presigned download URL for private blobs (1 hour expiry)
      const s3Client = new S3Client({
        region: "auto",
        endpoint: `https://${c.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
          accessKeyId: c.env.R2_ACCESS_KEY_ID || "",
          secretAccessKey: c.env.R2_SECRET_ACCESS_KEY || "",
        },
      });

      const command = new GetObjectCommand({
        Bucket: "federise-objects",
        Key: r2Key,
      });

      const url = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
      const expiresAt = new Date(Date.now() + 3600 * 1000).toISOString();

      return {
        url,
        metadata,
        expiresAt,
      };
    }
  }
}
