import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, ErrorResponse } from "../types";

export class PingEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Health"],
    summary: "Ping endpoint to check if the service is alive",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
    },
    responses: {
      "200": {
        description: "Service is alive",
        content: {
          "application/json": {
            schema: z.object({
              message: z.string(),
              timestamp: z.string(),
            })
          }
        },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    return c.json({
      message: "pong",
      timestamp: new Date().toISOString()
    });
  }
}
