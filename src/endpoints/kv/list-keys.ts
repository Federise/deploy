import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, ListKeysRequest, ErrorResponse } from "../../types";

export class KVListKeysEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Key-Value Operations"],
    summary: "List all keys in a namespace",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: ListKeysRequest } },
      },
    },
    responses: {
      "200": {
        description: "The request has succeeded.",
        content: { "application/json": { schema: z.array(z.string()) } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
      "404": {
        description: "Not found",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    const ns = data.body.namespace;
    const list = await c.env.KV.list({ prefix: `${ns}:` });
    return list.keys.map((k) => k.name.substring(ns.length + 1));
  }
}
