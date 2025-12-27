import { fromHono } from "chanfana";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { authMiddleware } from "./middleware/auth";
import { PingEndpoint } from "./endpoints/ping";
import { PrincipalListEndpoint } from "./endpoints/principal/list";
import { PrincipalCreateEndpoint } from "./endpoints/principal/create";
import { PrincipalDeleteEndpoint } from "./endpoints/principal/delete";
import { KVListNamespacesEndpoint } from "./endpoints/kv/list-namespaces";
import { KVListKeysEndpoint } from "./endpoints/kv/list-keys";
import { KVGetEndpoint } from "./endpoints/kv/get";
import { KVSetEndpoint } from "./endpoints/kv/set";
import { KVBulkGetEndpoint } from "./endpoints/kv/bulk-get";
import { KVBulkSetEndpoint } from "./endpoints/kv/bulk-set";
import { KVDumpEndpoint } from "./endpoints/kv/dump";
import { BlobUploadEndpoint } from "./endpoints/blob/upload";
import { BlobGetEndpoint } from "./endpoints/blob/get";
import { BlobDeleteEndpoint } from "./endpoints/blob/delete";
import { BlobListEndpoint } from "./endpoints/blob/list";

const app = new Hono<{ Bindings: Env }>();

app.use("*", (c, next) => {
  const origin = c.env.CORS_ORIGIN || "*";
  return cors({
    origin,
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
    exposeHeaders: ["Content-Length"],
    maxAge: 86400,
    credentials: false,
  })(c, next);
});
app.use("*", authMiddleware);

const openapi = fromHono(app, {
  docs_url: "/openapi",
});

// Health routes
openapi.get("/ping", PingEndpoint);

// Principal routes
openapi.post("/principal/list", PrincipalListEndpoint);
openapi.post("/principal/create", PrincipalCreateEndpoint);
openapi.post("/principal/delete", PrincipalDeleteEndpoint);

// KV routes
openapi.post("/kv/get", KVGetEndpoint);
openapi.post("/kv/set", KVSetEndpoint);
openapi.post("/kv/keys", KVListKeysEndpoint);
openapi.post("/kv/bulk/get", KVBulkGetEndpoint);
openapi.post("/kv/bulk/set", KVBulkSetEndpoint);
openapi.post("/kv/namespaces", KVListNamespacesEndpoint);
openapi.post("/kv/dump", KVDumpEndpoint);

// Blob routes
openapi.post("/blob/upload", BlobUploadEndpoint);
openapi.post("/blob/get", BlobGetEndpoint);
openapi.post("/blob/delete", BlobDeleteEndpoint);
openapi.post("/blob/list", BlobListEndpoint);

export default app;
