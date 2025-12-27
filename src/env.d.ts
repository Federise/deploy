declare interface Env {
  KV: KVNamespace;
  R2: R2Bucket;
  R2_PUBLIC: R2Bucket;
  BOOTSTRAP_API_KEY?: string;
  CORS_ORIGIN?: string;
  PUBLIC_DOMAIN?: string;
  R2_ACCOUNT_ID?: string;
  R2_ACCESS_KEY_ID?: string;
  R2_SECRET_ACCESS_KEY?: string;
}
