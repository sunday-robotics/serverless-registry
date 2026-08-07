import z from "zod";
import { Env } from "../..";
import { errorString } from "../utils";
import { R2Registry } from "./r2";
import { RegistryHTTPClient } from "./http";
import { registries } from "./registry";

// Message enqueued by the pull-through fallback when a proxied layer is too big to cache inline.
// It carries only a reference (Queue messages are size-limited), never the blob bytes; the consumer
// re-fetches the layer from the upstream registry in ranges.
export const blobCacheMessageSchema = z.object({
  registry: z.string(),
  name: z.string(),
  digest: z.string(),
});

export type BlobCacheMessage = z.infer<typeof blobCacheMessageSchema>;

export async function handleBlobCacheBatch(batch: MessageBatch<unknown>, env: Env): Promise<void> {
  for (const message of batch.messages) {
    const parsed = blobCacheMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      // A malformed message can never succeed, so drop it instead of retrying forever.
      console.error("Dropping invalid blob cache message:", errorString(parsed.error));
      message.ack();
      continue;
    }

    const { registry, name, digest } = parsed.data;
    try {
      await cacheLayer(env, registry, name, digest);
      message.ack();
    } catch (err) {
      console.error(`Failed to cache layer ${digest} for ${name}, will retry:`, errorString(err));
      message.retry();
    }
  }
}

async function cacheLayer(env: Env, registryURL: string, name: string, digest: string): Promise<void> {
  const r2 = new R2Registry(env);

  // Idempotency: another attempt (or the inline path) may have already cached it.
  const existing = await r2.layerExists(name, digest);
  if (!("response" in existing) && existing.exists) {
    return;
  }

  const configuration = registries(env).find((registry) => registry.registry === registryURL);
  if (configuration === undefined) {
    // The upstream registry is no longer configured; retrying can't help.
    console.warn(`No matching registry configuration for ${registryURL}, skipping cache of ${digest}`);
    return;
  }

  const client = new RegistryHTTPClient(env, configuration);
  const upstream = await client.layerExists(name, digest);
  if ("response" in upstream) {
    throw new Error(`upstream layerExists failed with status ${upstream.response.status}`);
  }

  if (!upstream.exists) {
    console.warn(`Upstream no longer has layer ${digest} for ${name}, skipping cache`);
    return;
  }

  const result = await r2.multipartUpload(name, digest, upstream.size, client);
  if ("response" in result) {
    throw new Error(`failed to cache layer ${digest}: status ${result.response.status}`);
  }
}
