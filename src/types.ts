import type { Context } from "hono";
import { z } from "zod";

export type AppContext = Context<{ Bindings: Env }>;

export const AuthorizationHeader = z.string().regex(/^ApiKey [a-zA-Z0-9]+$/);

export const Principal = z.object({
  secret_hash: z.string(),
  display_name: z.string(),
  created_at: z.string().datetime(),
  active: z.boolean(),
});

export const PrincipalList = z.object({
  items: z.array(Principal),
});

export const PrincipalCreateRequest = z.object({
  display_name: z.string(),
});

export const PrincipalCreateResponse = z.object({
  secret_hash: z.string(),
  display_name: z.string(),
  created_at: z.string().datetime(),
  active: z.boolean(),
  secret: z.string(),
});

export const NamespaceValue = z.string();

export const KVEntry = z.object({
  key: z.string(),
  value: z.string(),
});

export const ListKeysRequest = z.object({
  namespace: NamespaceValue,
});

export const GetRequest = z.object({
  namespace: NamespaceValue,
  key: z.string(),
});

export const SetRequest = z.object({
  namespace: NamespaceValue,
  key: z.string(),
  value: z.string(),
});

export const BulkGetRequest = z.object({
  namespace: NamespaceValue,
  keys: z.array(z.string()),
});

export const BulkGetResponse = z.object({
  entries: z.array(KVEntry),
});

export const BulkSetRequest = z.object({
  namespace: NamespaceValue,
  entries: z.array(KVEntry),
});

export const BulkSetResponse = z.object({
  success: z.boolean(),
  count: z.number().int(),
});

export const KVDumpEntry = z.object({
  namespace: NamespaceValue,
  entries: z.array(KVEntry),
});

// Blob metadata stored in KV
export const BlobMetadata = z.object({
  key: z.string(),
  namespace: NamespaceValue,
  size: z.number().int().positive(),
  contentType: z.string(),
  uploadedAt: z.string().datetime(),
  isPublic: z.boolean(),
});

// Response after successful upload (metadata headers used for request)
export const BlobUploadResponse = z.object({
  metadata: BlobMetadata,
});

// Request to get blob download URL
export const BlobGetRequest = z.object({
  namespace: NamespaceValue,
  key: z.string(),
});

// Response with download URL
export const BlobGetResponse = z.object({
  url: z.string().url(),
  metadata: BlobMetadata,
  expiresAt: z.string().datetime().optional(), // Only for private blobs
});

// Request to delete blob
export const BlobDeleteRequest = z.object({
  namespace: NamespaceValue,
  key: z.string(),
});

// Request to list blobs
export const BlobListRequest = z.object({
  namespace: NamespaceValue.optional(),
});

// Response with blob list
export const BlobListResponse = z.object({
  blobs: z.array(BlobMetadata),
});

export const ErrorResponse = z.object({
  code: z.number().int(),
  message: z.string(),
});
