import { AwsClient } from "aws4fetch";
import { Env } from "../..";
import { errorString, wrap } from "../utils";
import { symlinkHeader } from "./r2";

const defaultTtlSeconds = 10 * 60;
const maxTtlSeconds = 7 * 24 * 60 * 60; // SigV4 limit on X-Amz-Expires
const requiredVars = [
  "R2_PRESIGN_ACCOUNT_ID",
  "R2_PRESIGN_BUCKET",
  "R2_PRESIGN_ACCESS_KEY_ID",
  "R2_PRESIGN_SECRET_ACCESS_KEY",
] as const;

export type PresignConfig = {
  endpoint: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  ttlSeconds: number;
};

// undefined means redirects are off. Setting only some of the required vars is a misconfiguration,
// not an opt-out, so it is logged and treated as off.
export function presignConfig(env: Env): PresignConfig | undefined {
  const missing = requiredVars.filter((name) => !env[name]);
  if (missing.length === requiredVars.length) {
    return undefined;
  }
  if (missing.length > 0) {
    console.error(`Presigned redirects are misconfigured, missing: ${missing.join(", ")}`);
    return undefined;
  }

  const ttlSeconds = Number(env.R2_PRESIGN_TTL_SECONDS ?? defaultTtlSeconds);

  return {
    endpoint: `https://${env.R2_PRESIGN_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    bucket: env.R2_PRESIGN_BUCKET!,
    accessKeyId: env.R2_PRESIGN_ACCESS_KEY_ID!,
    secretAccessKey: env.R2_PRESIGN_SECRET_ACCESS_KEY!,
    ttlSeconds:
      Number.isInteger(ttlSeconds) && ttlSeconds > 0 ? Math.min(ttlSeconds, maxTtlSeconds) : defaultTtlSeconds,
  };
}

// Presigned R2 URL for a layer stored in the bucket, or null when redirects are off or the layer
// has to be served through the Worker (not in R2, or the R2 lookup failed).
export async function presignedLayerUrl(env: Env, name: string, digest: string): Promise<string | null> {
  const config = presignConfig(env);
  if (config === undefined) {
    return null;
  }

  const [head, err] = await wrap(env.REGISTRY.head(`${name}/blobs/${digest}`));
  if (err) {
    console.error("Error looking up layer for presigned redirect:", errorString(err));
    return null;
  }
  if (!head) {
    return null;
  }

  // A mounted layer is a pointer whose metadata names the repo holding the bytes.
  // mountExistingLayer never points at another pointer, so one hop is enough.
  const sourceName = head.customMetadata?.[symlinkHeader] ?? name;

  return await presignedGetUrl(config, `${sourceName}/blobs/${digest}`);
}

async function presignedGetUrl(config: PresignConfig, key: string): Promise<string> {
  const client = new AwsClient({
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    service: "s3",
    region: "auto",
  });

  const url = new URL(`${config.endpoint}/${config.bucket}/${key}`);
  url.searchParams.set("X-Amz-Expires", `${config.ttlSeconds}`);

  const signed = await client.sign(new Request(url, { method: "GET" }), { aws: { signQuery: true } });
  return signed.url;
}
