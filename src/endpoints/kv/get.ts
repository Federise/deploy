import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, GetRequest, KVEntry, ErrorResponse } from "../../types";

export class KVGetEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Key-Value Operations"],
    summary: "Get a value by key",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: GetRequest } },
      },
    },
    responses: {
      "200": {
        description: "The request has succeeded.",
        content: { "application/json": { schema: KVEntry } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "404": {
        description: "Not found",
        content: { "application/json": { schema: ErrorResponse } },
      }
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const value = await c.env.KV.get(`${data.body.namespace}:${data.body.key}`);
    if (value === null) {
      return c.json({ code: 404, message: "Key not found" }, 404);
    }
    return { key: data.body.key, value };
  }
}
