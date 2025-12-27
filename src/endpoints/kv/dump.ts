import { z } from "zod";
import { OpenAPIRoute } from "chanfana";

import { type AppContext, AuthorizationHeader, KVDumpEntry, ErrorResponse } from "../../types";

export class KVDumpEndpoint extends OpenAPIRoute {
  schema = {
    tags: ["Key-Value Operations"],
    summary: "Dump all key-value pairs in all namespaces",
    request: {
      headers: z.object({
        authorization: AuthorizationHeader,
      }),
    },
    responses: {
      "200": {
        description: "The request has succeeded.",
        content: { "application/json": { schema: z.array(KVDumpEntry) } },
      },
      "401": {
        description: "Unauthorized",
        content: { "application/json": { schema: ErrorResponse } },
      },
    },
  };

  async handle(c: AppContext) {
    const list = await c.env.KV.list();
    const namespaceMap = new Map<string, Array<{ key: string; value: string }>>();

    const validKeys = list.keys.filter((k) => {
      const parts = k.name.split(":");
      return parts.length >= 2 && !parts[0].startsWith("__");
    });

    const values = await Promise.all(
      validKeys.map((k) => c.env.KV.get(k.name))
    );

    validKeys.forEach((k, i) => {
      const parts = k.name.split(":");
      const ns = parts[0];
      const key = parts.slice(1).join(":");
      const value = values[i];

      if (!namespaceMap.has(ns)) {
        namespaceMap.set(ns, []);
      }
      const entries = namespaceMap.get(ns);
      if (entries) {
        entries.push({ key, value: value || "" });
      }
    });

    return Array.from(namespaceMap.entries())
      .map(([namespace, entries]) => ({ namespace, entries }))
      .sort((a, b) => a.namespace.localeCompare(b.namespace));
  }
}
