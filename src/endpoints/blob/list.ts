import { z } from "zod";
import { OpenAPIRoute } from "chanfana";
import {
  type AppContext,
  AuthorizationHeader,
  BlobListRequest,
  BlobListResponse,
  ErrorResponse,
} from "../../types";

export class BlobListEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Blob Operations"],
    summary: "List blobs",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: BlobListRequest } },
      },
    },
    responses: {
      "200": {
        description: "List of blobs",
        content: { "application/json": { schema: BlobListResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const { namespace } = data.body;

    // Build prefix for KV list
    const prefix = namespace ? `__BLOB:${namespace}:` : "__BLOB:";

    // List all blob metadata from KV
    const list = await c.env.KV.list({ prefix });

    // Fetch all metadata
    const metadataPromises = list.keys.map(async (item) => {
      const value = await c.env.KV.get(item.name);
      return value ? JSON.parse(value) : null;
    });

    const blobs = (await Promise.all(metadataPromises)).filter(
      (m) => m !== null
    );

    return { blobs };
  }
}
