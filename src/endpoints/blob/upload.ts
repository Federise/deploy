import { OpenAPIRoute } from "chanfana";
import { z } from "zod";
import {
  type AppContext,
  AuthorizationHeader,
  BlobMetadata,
  ErrorResponse,
} from "../../types";

// Response after successful upload
const BlobUploadResponse = z.object({
  metadata: BlobMetadata,
});

export class BlobUploadEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "Upload a blob directly to storage",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
        "content-type": z.string(),
        "x-blob-namespace": z.string().describe("Namespace for the blob"),
        "x-blob-key": z.string().describe("Key/filename for the blob"),
        "x-blob-public": z.string().optional().describe("Set to 'true' for public storage"),
      }),
    },
    responses: {
      "200": {
        description: "Blob uploaded successfully",
        content: { "application/json": { schema: BlobUploadResponse } },
      },
      "400": {
        description: "Bad request",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const namespace = c.req.header("x-blob-namespace");
    const key = c.req.header("x-blob-key");
    const isPublic = c.req.header("x-blob-public") === "true";
    const contentType = c.req.header("content-type") || "application/octet-stream";

    if (!namespace || !key) {
      return c.json({ code: 400, message: "Missing x-blob-namespace or x-blob-key header" }, 400);
    }

    // Get the raw body
    const body = await c.req.arrayBuffer();
    const size = body.byteLength;

    if (size === 0) {
      return c.json({ code: 400, message: "Empty file" }, 400);
    }

    // Select the appropriate bucket
    const bucket = isPublic ? c.env.R2_PUBLIC : c.env.R2;
    const r2Key = `${namespace}:${key}`;

    // Upload to R2
    await bucket.put(r2Key, body, {
      httpMetadata: {
        contentType,
      },
    });

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

    return c.json({ metadata });
  }
}
