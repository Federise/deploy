import { z } from "zod";
import { OpenAPIRoute } from "chanfana";
import {
  type AppContext,
  AuthorizationHeader,
  BlobDeleteRequest,
  ErrorResponse,
} from "../../types";

export class BlobDeleteEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "Delete a blob",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: BlobDeleteRequest } },
      },
    },
    responses: {
      "204": {
        description: "Blob deleted successfully",
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

    // Get metadata to determine which bucket
    const kvKey = `__BLOB:${namespace}:${key}`;
    const metadataStr = await c.env.KV.get(kvKey);

    if (!metadataStr) {
      return c.json({ code: 404, message: "Blob not found" }, 404);
    }

    const metadata = JSON.parse(metadataStr);
    const r2Key = `${namespace}:${key}`;

    // Delete from appropriate R2 bucket
    const bucket = metadata.isPublic ? c.env.R2_PUBLIC : c.env.R2;
    await bucket.delete(r2Key);

    // Delete metadata from KV
    await c.env.KV.delete(kvKey);

    return new Response(null, { status: 204 });
  }
}
