import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, PrincipalList, ErrorResponse } from "../../types";

export class PrincipalListEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Principal Management"],
    summary: "List all principals",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
    },
    responses: {
      "200": {
        description: "The request has succeeded.",
        content: { "application/json": { schema: PrincipalList } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      }
    },
  };

  async handle(c: AppContext) {
    const list = await c.env.KV.list({ prefix: "__PRINCIPAL:" });
    const items = (
      await Promise.all(
        list.keys.map(async (k) => {
          const val = await c.env.KV.get(k.name);
          if (!val) return null;
          try {
            const principal = JSON.parse(val);
            const secret_hash = k.name.replace("__PRINCIPAL:", "");
            return {
              secret_hash,
              display_name: principal.display_name,
              created_at: principal.created_at,
              active: principal.active,
            };
          } catch {
            return null;
          }
        })
      )
    ).filter((item): item is NonNullable<typeof item> => item !== null);

    return { items };
  }
}
