import { AwsClient } from "aws4fetch";
import { Env } from "../..";
import { symlinkHeader } from "./r2";

const defaultPresignTtlSeconds = 600;

type PresignConfig = {
  client: AwsClient;
  endpoint: string;
  ttlSeconds: number;
};

// Presigning is on only when all four R2_PRESIGN_* values are set.
function presignConfig(env: Env): PresignConfig | undefined {
  const { R2_PRESIGN_ACCOUNT_ID, R2_PRESIGN_BUCKET, R2_PRESIGN_ACCESS_KEY_ID, R2_PRESIGN_SECRET_ACCESS_KEY } = env;
  if (!R2_PRESIGN_ACCOUNT_ID || !R2_PRESIGN_BUCKET || !R2_PRESIGN_ACCESS_KEY_ID || !R2_PRESIGN_SECRET_ACCESS_KEY) {
    return undefined;
  }

  const ttlSeconds = Number(env.R2_PRESIGN_TTL_SECONDS ?? defaultPresignTtlSeconds);
  return {
    client: new AwsClient({
      accessKeyId: R2_PRESIGN_ACCESS_KEY_ID,
      secretAccessKey: R2_PRESIGN_SECRET_ACCESS_KEY,
      service: "s3",
      region: "auto",
    }),
    endpoint: `https://${R2_PRESIGN_ACCOUNT_ID}.r2.cloudflarestorage.com/${R2_PRESIGN_BUCKET}`,
    ttlSeconds: Number.isInteger(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds : defaultPresignTtlSeconds,
  };
}

/**
 * Presigned R2 S3-API URL for a cached layer, or null when presigning is off or the layer isn't in R2.
 *
 * Clients follow the redirect and download from R2 directly, so layer bytes skip the Worker.
 * A mounted layer is a symlink object whose metadata names the source repo:
 *   sunday-robotics/a/blobs/sha256:ab12…  { X-Serverless-Registry-Symlink: "sunday-robotics/b" }
 *   → presign sunday-robotics/b/blobs/sha256:ab12…
 */
export async function presignedLayerUrl(env: Env, name: string, digest: string): Promise<string | null> {
  const config = presignConfig(env);
  if (config === undefined) return null;

  const head = await env.REGISTRY.head(`${name}/blobs/${digest}`);
  if (head === null) return null;

  // mountExistingLayer always points at the original, so one hop is enough
  const sourceName = head.customMetadata?.[symlinkHeader] ?? name;

  const url = new URL(`${config.endpoint}/${sourceName}/blobs/${digest}`);
  url.searchParams.set("X-Amz-Expires", `${config.ttlSeconds}`);
  const signed = await config.client.sign(new Request(url, { method: "GET" }), { aws: { signQuery: true } });
  return signed.url;
}
