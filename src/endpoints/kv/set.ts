import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, ErrorResponse, SetRequest } from "../../types";

export class KVSetEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Key-Value Operations"],
    summary: "Set a key-value pair",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: { "application/json": { schema: SetRequest } },
      },
    },
    responses: {
      "204": { description: "No content" },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const data = await this.getValidatedData<typeof this.schema>();
    await c.env.KV.put(`${data.body.namespace}:${data.body.key}`, data.body.value);
    return new Response(null, { status: 204 });
  }
}
