import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, ErrorResponse } from "../../types";

export class PrincipalDeleteEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Principal Management"],
    summary: "Delete a principal",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
      body: {
        content: {
          "application/json": {
            schema: z.object({ secret_hash: z.string() }),
          },
        },
      },
    },
    responses: {
      "204": { description: "No content" },
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
    const val = await c.env.KV.get(`__PRINCIPAL:${data.body.secret_hash}`);
    if (!val) {
      return c.json({ code: 404, message: "Not found" }, 404);
    }
    await c.env.KV.delete(`__PRINCIPAL:${data.body.secret_hash}`);  
    return new Response(null, { status: 204 });
  }
}
