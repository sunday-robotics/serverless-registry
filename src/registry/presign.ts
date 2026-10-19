import { AwsClient } from "aws4fetch";
import { Env } from "../..";
import { symlinkHeader } from "./r2";

const defaultPresignTtlSeconds = 600;

type PresignConfig = {
  client: AwsClient;
  endpoint: string;
  ttlSeconds: number;
};

function presignConfig(env: Env): PresignConfig | undefined {
  const { R2_PRESIGN_ACCOUNT_ID, R2_PRESIGN_BUCKET, R2_PRESIGN_ACCESS_KEY_ID, R2_PRESIGN_SECRET_ACCESS_KEY } = env;

  // Require all env vars to be set to use presigned URL
  if (!R2_PRESIGN_ACCOUNT_ID || !R2_PRESIGN_BUCKET || !R2_PRESIGN_ACCESS_KEY_ID || !R2_PRESIGN_SECRET_ACCESS_KEY) {
    return undefined;
  }

  // Get ttl
  const ttlSeconds = Number(env.R2_PRESIGN_TTL_SECONDS ?? defaultPresignTtlSeconds);
  const ttlAsNum = Number.isInteger(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds : defaultPresignTtlSeconds;

  return {
    client: new AwsClient({
      accessKeyId: R2_PRESIGN_ACCESS_KEY_ID,
      secretAccessKey: R2_PRESIGN_SECRET_ACCESS_KEY,
      service: "s3",
      region: "auto",
    }),
    endpoint: `https://${R2_PRESIGN_ACCOUNT_ID}.r2.cloudflarestorage.com/${R2_PRESIGN_BUCKET}`,
    ttlSeconds: ttlAsNum,
  };
}

export async function presignedLayerUrl(env: Env, name: string, digest: string): Promise<string | null> {
  // Load config from environment
  const config = presignConfig(env);
  if (config === undefined) {
    return null;
  }

  // Fetch the r2 metadata of the image from the registry
  const head = await env.REGISTRY.head(`${name}/blobs/${digest}`);
  if (head === null) {
    return null;
  }

  // A mounted layer is a pointer whose metadata names the repo holding the bytes.
  // mountExistingLayer never points at another pointer, so one hop is enough
  const sourceName = head.customMetadata?.[symlinkHeader] ?? name;

  // Create the URL and set the ttl
  const url = new URL(`${config.endpoint}/${sourceName}/blobs/${digest}`);
  url.searchParams.set("X-Amz-Expires", `${config.ttlSeconds}`);

  const signed = await config.client.sign(new Request(url, { method: "GET" }), { aws: { signQuery: true } });
  return signed.url;
}
