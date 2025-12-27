import { Context, Next } from "hono";
import { hashApiKey } from "../lib/crypto";

export async function authMiddleware(c: Context<{ Bindings: Env }>, next: Next) {
  // Skip authentication for OpenAPI documentation
  if (c.req.path.startsWith("/openapi")) {
    await next();
    return;
  }

  const auth = c.req.header("authorization");

  if (!auth || !auth.match(/^ApiKey [a-zA-Z0-9]+$/)) {
    return c.json({ code: 401, message: "Invalid authorization header" }, 401);
  }

  const apiKey = auth.replace("ApiKey ", "");

  // Check if using bootstrap key (only valid when no principals exist and on the /principal/create endpoint)
  if (c.env.BOOTSTRAP_API_KEY && apiKey === c.env.BOOTSTRAP_API_KEY) {

    if (!c.req.path.startsWith("/principal/create")) {
      return c.json({ code: 401, message: "Unauthorized" }, 401);
    }

    // should probably alert on this
    const list = await c.env.KV.list({ prefix: "__PRINCIPAL:" });
    if (list.keys.length === 0) {
      // No principals exist, bootstrap key is valid
      await next();
      return;
    }
    // Principals exist, bootstrap key is no longer valid
    return c.json({ code: 401, message: "Unauthorized" }, 401);
  }

  // Check regular principal auth
  const apiKeyHash = await hashApiKey(apiKey);
  const val = await c.env.KV.get(`__PRINCIPAL:${apiKeyHash}`);

  if (!val) {
    return c.json({ code: 401, message: "Unauthorized" }, 401);
  }

  let principal: { active: boolean };
  try {
    principal = JSON.parse(val);
  } catch {
    return c.json({ code: 401, message: "Unauthorized" }, 401);
  }

  if (!principal.active) {
    return c.json({ code: 401, message: "Unauthorized" }, 401);
  }

  await next();
}
