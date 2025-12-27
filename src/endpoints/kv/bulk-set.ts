import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, BulkSetRequest, BulkSetResponse, ErrorResponse } from "../../types";

export class KVBulkSetEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Key-Value Operations"],
    summary: "Bulk set values",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: BulkSetRequest } },
      },
    },
    responses: {
      "200": {
        description: "The request has succeeded.",
        content: { "application/json": { schema: BulkSetResponse } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const ns = data.body.namespace;
    await Promise.all(data.body.entries.map((e) => c.env.KV.put(`${ns}:${e.key}`, e.value)));
    return { success: true, count: data.body.entries.length };
  }
}
