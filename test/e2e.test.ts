import { env, SELF } from "cloudflare:test";
import { describe, it, expect, beforeEach } from "vitest";

declare module "cloudflare:test" {
  interface ProvidedEnv extends Env {}
}

describe("KV Gateway E2E Tests", () => {
  const BOOTSTRAP_API_KEY = "testbootstrapkey123";
  let adminApiKey: string;

  describe("Bootstrap & Principal Management", () => {
    beforeEach(async () => {
      // Clear KV before each test
      const list = await env.KV.list();
      await Promise.all(list.keys.map((k: { name: string }) => env.KV.delete(k.name)));
    });

    it("should create first principal with bootstrap key", async () => {
      const response = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("secret");
      expect(data).toHaveProperty("secret_hash");
      expect(data.display_name).toBe("Admin");
      expect(data.active).toBe(true);

      adminApiKey = data.secret;
    });

    it("should list principals", async () => {
      // Create a principal first with bootstrap key
      const createRes = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });
      const admin = await createRes.json() as any;
      adminApiKey = admin.secret;

      const response = await SELF.fetch("http://localhost/principal/list", {
        method: "POST",
        headers: { Authorization: `ApiKey ${adminApiKey}` },
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("items");
      expect(data.items).toHaveLength(1);
      expect(data.items[0].display_name).toBe("Admin");
      expect(data.items[0]).toHaveProperty("secret_hash");
      expect(data.items[0]).not.toHaveProperty("secret");
    });

    it("should create additional principals when authenticated", async () => {
      // Bootstrap admin with bootstrap key
      const bootstrap = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });
      const admin = await bootstrap.json() as any;
      adminApiKey = admin.secret;

      const response = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ display_name: "User" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.display_name).toBe("User");
      expect(data).toHaveProperty("secret");
    });

    it("should delete a principal", async () => {
      // Bootstrap admin with bootstrap key
      const bootstrap = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });
      const admin = await bootstrap.json() as any;
      adminApiKey = admin.secret;

      // Create user to delete
      const createRes = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ display_name: "ToDelete" }),
      });
      const user = await createRes.json() as any;

      const response = await SELF.fetch("http://localhost/principal/delete", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ secret_hash: user.secret_hash }),
      });

      expect(response.status).toBe(204);

      // Verify deleted
      const listRes = await SELF.fetch("http://localhost/principal/list", {
        method: "POST",
        headers: { Authorization: `ApiKey ${adminApiKey}` },
      });
      const list = await listRes.json() as any;
      expect(list.items).toHaveLength(1);
    });

    it("should reject requests without valid auth", async () => {
      const response = await SELF.fetch("http://localhost/principal/list", {
        method: "POST",
        headers: { Authorization: "ApiKey invalid" },
      });

      expect(response.status).toBe(401);
    });

    it("should reject requests with malformed auth header", async () => {
      const response = await SELF.fetch("http://localhost/principal/list", {
        method: "POST",
        headers: { Authorization: "Bearer invalid" },
      });

      expect(response.status).toBe(401);
    });

    it("should reject bootstrap key for non-create endpoints after principals exist", async () => {
      // Create a principal first
      await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });

      // Try to use bootstrap key for list endpoint (should be rejected)
      const response = await SELF.fetch("http://localhost/principal/list", {
        method: "POST",
        headers: { Authorization: `ApiKey ${BOOTSTRAP_API_KEY}` },
      });

      expect(response.status).toBe(401);
    });
  });

  describe("KV Operations", () => {
    beforeEach(async () => {
      // Clear KV and bootstrap admin for each test
      const list = await env.KV.list();
      await Promise.all(list.keys.map((k: { name: string }) => env.KV.delete(k.name)));

      const bootstrap = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });
      const admin = await bootstrap.json() as any;
      adminApiKey = admin.secret;
    });

    it("should set a key-value pair", async () => {
      const response = await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo", value: "bar" }),
      });

      expect(response.status).toBe(204);
    });

    it("should get a key-value pair", async () => {
      // Set first
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo", value: "bar" }),
      });

      const response = await SELF.fetch("http://localhost/kv/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.key).toBe("foo");
      expect(data.value).toBe("bar");
    });

    it("should return 404 for non-existent key", async () => {
      const response = await SELF.fetch("http://localhost/kv/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "nonexistent" }),
      });

      expect(response.status).toBe(404);
    });

    it("should accept namespace with uri-safe characters", async () => {
      const response = await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "example.com:8080", key: "foo", value: "bar" }),
      });

      expect(response.status).toBe(204);

      // Verify it was set
      const getRes = await SELF.fetch("http://localhost/kv/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "example.com:8080", key: "foo" }),
      });

      expect(getRes.status).toBe(200);
    });

    it("should list keys in a namespace", async () => {
      // Set multiple keys
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo", value: "bar" }),
      });
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "baz", value: "qux" }),
      });

      const response = await SELF.fetch("http://localhost/kv/keys", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toEqual(expect.arrayContaining(["foo", "baz"]));
    });

    it("should list namespaces", async () => {
      // Set keys in different namespaces
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "app1", key: "foo", value: "bar" }),
      });
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "app2", key: "baz", value: "qux" }),
      });

      const response = await SELF.fetch("http://localhost/kv/namespaces", {
        method: "POST",
        headers: { Authorization: `ApiKey ${adminApiKey}` },
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toEqual(expect.arrayContaining(["app1", "app2"]));
      expect(data).not.toContain("__PRINCIPAL");
    });

    it("should bulk get values", async () => {
      // Set multiple keys
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo", value: "bar" }),
      });
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "baz", value: "qux" }),
      });

      const response = await SELF.fetch("http://localhost/kv/bulk/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", keys: ["foo", "baz", "nonexistent"] }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.entries).toHaveLength(2);
      expect(data.entries).toEqual(
        expect.arrayContaining([
          { key: "foo", value: "bar" },
          { key: "baz", value: "qux" },
        ])
      );
    });

    it("should bulk set values", async () => {
      const response = await SELF.fetch("http://localhost/kv/bulk/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          entries: [
            { key: "foo", value: "bar" },
            { key: "baz", value: "qux" },
          ],
        }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.success).toBe(true);
      expect(data.count).toBe(2);

      // Verify values were set
      const getRes = await SELF.fetch("http://localhost/kv/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo" }),
      });
      const getValue = await getRes.json() as any;
      expect(getValue.value).toBe("bar");
    });

    it("should dump all KV data", async () => {
      // Set keys in multiple namespaces
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "app1", key: "foo", value: "bar" }),
      });
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "app2", key: "baz", value: "qux" }),
      });

      const response = await SELF.fetch("http://localhost/kv/dump", {
        method: "POST",
        headers: { Authorization: `ApiKey ${adminApiKey}` },
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveLength(2);
      expect(data).toEqual(
        expect.arrayContaining([
          {
            namespace: "app1",
            entries: [{ key: "foo", value: "bar" }],
          },
          {
            namespace: "app2",
            entries: [{ key: "baz", value: "qux" }],
          },
        ])
      );
    });

    it("should handle keys with colons", async () => {
      await SELF.fetch("http://localhost/kv/set", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo:bar:baz", value: "test" }),
      });

      const response = await SELF.fetch("http://localhost/kv/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "foo:bar:baz" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.key).toBe("foo:bar:baz");
      expect(data.value).toBe("test");
    });
  });

  describe("Blob Operations", () => {
    beforeEach(async () => {
      // Clear KV and bootstrap admin for each test
      const list = await env.KV.list();
      await Promise.all(list.keys.map((k: { name: string }) => env.KV.delete(k.name)));

      const bootstrap = await SELF.fetch("http://localhost/principal/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `ApiKey ${BOOTSTRAP_API_KEY}`,
        },
        body: JSON.stringify({ display_name: "Admin" }),
      });
      const admin = await bootstrap.json() as any;
      adminApiKey = admin.secret;
    });

    it("should initiate private blob upload and return presigned URL", async () => {
      const response = await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "test-file.txt",
          contentType: "text/plain",
          size: 1024,
          isPublic: false,
        }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("uploadUrl");
      expect(data).toHaveProperty("expiresAt");
      expect(data.uploadUrl).toContain("federise-objects");

      // Verify metadata was created in KV
      const metadata = await env.KV.get("__BLOB:myapp:test-file.txt");
      expect(metadata).toBeTruthy();
      const parsed = JSON.parse(metadata!);
      expect(parsed.isPublic).toBe(false);
      expect(parsed.size).toBe(1024);
      expect(parsed.contentType).toBe("text/plain");
    });

    it("should initiate public blob upload and return presigned URL", async () => {
      const response = await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "public-image.png",
          contentType: "image/png",
          size: 2048,
          isPublic: true,
        }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("uploadUrl");
      expect(data).toHaveProperty("expiresAt");
      expect(data.uploadUrl).toContain("federise-objects-public");

      // Verify metadata was created in KV
      const metadata = await env.KV.get("__BLOB:myapp:public-image.png");
      expect(metadata).toBeTruthy();
      const parsed = JSON.parse(metadata!);
      expect(parsed.isPublic).toBe(true);
    });

    it("should get private blob download URL with expiry", async () => {
      // Create blob metadata first
      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "private-doc.pdf",
          contentType: "application/pdf",
          size: 1024,
          isPublic: false,
        }),
      });

      const response = await SELF.fetch("http://localhost/blob/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "private-doc.pdf" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("url");
      expect(data).toHaveProperty("metadata");
      expect(data).toHaveProperty("expiresAt");
      expect(data.metadata.isPublic).toBe(false);
      expect(data.url).toContain("federise-objects");
    });

    it("should get public blob custom domain URL without expiry", async () => {
      // Create blob metadata first
      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "public-banner.jpg",
          contentType: "image/jpeg",
          size: 2048,
          isPublic: true,
        }),
      });

      const response = await SELF.fetch("http://localhost/blob/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "public-banner.jpg" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data).toHaveProperty("url");
      expect(data).toHaveProperty("metadata");
      expect(data.expiresAt).toBeUndefined();
      expect(data.metadata.isPublic).toBe(true);
      expect(data.url).toContain("cdn.example.com");
      expect(data.url).toContain("myapp:public-banner.jpg");
    });

    it("should return 404 for non-existent blob", async () => {
      const response = await SELF.fetch("http://localhost/blob/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "nonexistent.txt" }),
      });

      expect(response.status).toBe(404);
      const data = await response.json() as any;
      expect(data.message).toBe("Blob not found");
    });

    it("should list blobs in a namespace", async () => {
      // Create multiple blobs
      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "file1.txt",
          contentType: "text/plain",
          size: 1024,
          isPublic: false,
        }),
      });

      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "file2.txt",
          contentType: "text/plain",
          size: 2048,
          isPublic: true,
        }),
      });

      const response = await SELF.fetch("http://localhost/blob/list", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp" }),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.blobs).toHaveLength(2);
      expect(data.blobs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ key: "file1.txt", isPublic: false }),
          expect.objectContaining({ key: "file2.txt", isPublic: true }),
        ])
      );
    });

    it("should list all blobs across namespaces", async () => {
      // Create blobs in different namespaces
      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "app1",
          key: "file.txt",
          contentType: "text/plain",
          size: 1024,
          isPublic: false,
        }),
      });

      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "app2",
          key: "image.png",
          contentType: "image/png",
          size: 2048,
          isPublic: true,
        }),
      });

      const response = await SELF.fetch("http://localhost/blob/list", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const data = await response.json() as any;
      expect(data.blobs).toHaveLength(2);
      expect(data.blobs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ namespace: "app1", key: "file.txt" }),
          expect.objectContaining({ namespace: "app2", key: "image.png" }),
        ])
      );
    });

    it("should delete a blob", async () => {
      // Create blob first
      await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "to-delete.txt",
          contentType: "text/plain",
          size: 1024,
          isPublic: false,
        }),
      });

      // Verify it exists
      const metadata = await env.KV.get("__BLOB:myapp:to-delete.txt");
      expect(metadata).toBeTruthy();

      const response = await SELF.fetch("http://localhost/blob/delete", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "to-delete.txt" }),
      });

      expect(response.status).toBe(204);

      // Verify metadata was deleted
      const deletedMetadata = await env.KV.get("__BLOB:myapp:to-delete.txt");
      expect(deletedMetadata).toBeNull();
    });

    it("should return 404 when deleting non-existent blob", async () => {
      const response = await SELF.fetch("http://localhost/blob/delete", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "nonexistent.txt" }),
      });

      expect(response.status).toBe(404);
      const data = await response.json() as any;
      expect(data.message).toBe("Blob not found");
    });

    it("should handle blob keys with special characters", async () => {
      const response = await SELF.fetch("http://localhost/blob/upload", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          namespace: "myapp",
          key: "folder/subfolder/file-name_v2.txt",
          contentType: "text/plain",
          size: 1024,
          isPublic: false,
        }),
      });

      expect(response.status).toBe(200);

      // Verify it can be retrieved
      const getRes = await SELF.fetch("http://localhost/blob/get", {
        method: "POST",
        headers: {
          Authorization: `ApiKey ${adminApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ namespace: "myapp", key: "folder/subfolder/file-name_v2.txt" }),
      });

      expect(getRes.status).toBe(200);
      const data = await getRes.json() as any;
      expect(data.metadata.key).toBe("folder/subfolder/file-name_v2.txt");
    });
  });
});
