import z from "zod";
import { Env } from "../..";
import { errorString } from "../utils";
import { isValidDigest } from "../user";
import { limit, MINIMUM_CHUNK, MAXIMUM_CHUNK } from "../chunk";
import { RegistryHTTPClient } from "./http";
import { registries, Registry } from "./registry";

// Default name of the queue this consumer serves; must match the `queue` field in the wrangler config.
// Used to guard the shared `queue()` handler against messages from other queues added in the future.
export const DEFAULT_BLOB_CACHE_QUEUE_NAME = "blob-cache";

// Resolves the queue name this deployment consumes. Deployments that share a Cloudflare account can
// override it via the BLOB_CACHE_QUEUE_NAME var so each uses a distinct, non-colliding queue.
export function blobCacheQueueName(env: Env): string {
  return env.BLOB_CACHE_QUEUE_NAME ?? DEFAULT_BLOB_CACHE_QUEUE_NAME;
}

// R2 hard limit on the number of parts in a single multipart upload.
const R2_MAX_PARTS = 10000;

// Defaults for the tunable knobs. All can be overridden through env vars (see blobCacheConfig).
// Part size: 256MiB keeps a single "upload-part" invocation to one ~256MiB range fetch + one R2
// uploadPart, which streams (never buffers) and finishes well within a Worker invocation's
// CPU/wall-clock budget. A 5GiB layer becomes ~20 parts; the 128MB memory cap is never approached
// because bytes flow straight from the upstream response into R2 through a FixedLengthStream.
const DEFAULT_PART_SIZE = 256 * 1024 * 1024;
// A tracked upload with no new part activity for this long is considered stalled and is aborted +
// restarted by the cron cleanup. 5 minutes comfortably exceeds the time to move a single part.
const DEFAULT_STALE_ABORT_MS = 5 * 60 * 1000;
// How many "upload-part" messages we keep in flight per blob (sliding window). Bounds the fan-out
// and the number of concurrent upstream range fetches for a single large layer.
const DEFAULT_MAX_CONCURRENT_PARTS = 6;

export type BlobCacheConfig = {
  partSize: number;
  staleAbortMs: number;
  maxConcurrentParts: number;
};

function positiveIntFromEnv(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

export function blobCacheConfig(env: Env): BlobCacheConfig {
  let partSize = positiveIntFromEnv(env.BLOB_CACHE_PART_SIZE, DEFAULT_PART_SIZE);
  // Respect R2's per-part bounds so the configured value can never produce an illegal upload.
  partSize = Math.min(Math.max(partSize, MINIMUM_CHUNK), MAXIMUM_CHUNK);
  return {
    partSize,
    staleAbortMs: positiveIntFromEnv(env.BLOB_CACHE_STALE_ABORT_MS, DEFAULT_STALE_ABORT_MS),
    maxConcurrentParts: Math.max(
      1,
      positiveIntFromEnv(env.BLOB_CACHE_MAX_CONCURRENT_PARTS, DEFAULT_MAX_CONCURRENT_PARTS),
    ),
  };
}

// Message enqueued to drive the background caching of a pull-through layer that is too big to cache
// inline. Messages carry only references (Queue messages are size-limited), never blob bytes.
//
// `type` discriminates the step. It defaults to "start" so messages produced by older deployments
// (which only sent { registry, name, digest }) are still interpreted correctly after an upgrade.
export const blobCacheMessageSchema = z.object({
  type: z.enum(["start", "upload-part", "finalize"]).default("start"),
  registry: z.string(),
  name: z.string(),
  digest: z.string(),
  // Only present for "upload-part" messages.
  partNumber: z.number().int().positive().optional(),
});

export type BlobCacheMessage = z.infer<typeof blobCacheMessageSchema>;

// Per-upload metadata: the single source of truth for resuming a chunked cache upload. Stored under
// a reserved, listable prefix so the cron cleanup can enumerate every in-flight upload.
const uploadMetadataSchema = z.object({
  version: z.literal(1),
  registry: z.string(),
  name: z.string(),
  digest: z.string(),
  // Final content-addressable key the multipart upload writes to directly.
  key: z.string(),
  // R2 multipart upload id used to resume/complete/abort.
  uploadId: z.string(),
  totalSize: z.number().int().positive(),
  partSize: z.number().int().positive(),
  totalParts: z.number().int().positive(),
  status: z.enum(["in-progress", "completed", "aborted"]),
  createdAt: z.number().int(),
  updatedAt: z.number().int(),
});

type UploadMetadata = z.infer<typeof uploadMetadataSchema>;

// Reserved key prefix. Kept out of the `<name>/manifests|blobs/...` layout so it never shows up in
// repository listings and never collides with real blob/manifest objects.
const CACHE_PREFIX = "_blob_cache";

function metaKey(name: string, digest: string): string {
  return `${CACHE_PREFIX}/${name}/${digest}/meta.json`;
}

function partsPrefix(name: string, digest: string): string {
  return `${CACHE_PREFIX}/${name}/${digest}/parts/`;
}

function partKey(name: string, digest: string, partNumber: number): string {
  return `${partsPrefix(name, digest)}${partNumber}`;
}

function finalKey(name: string, digest: string): string {
  return `${name}/blobs/${digest}`;
}

function partRange(meta: UploadMetadata, partNumber: number): { offset: number; end: number; length: number } {
  const offset = (partNumber - 1) * meta.partSize;
  const end = Math.min(partNumber * meta.partSize, meta.totalSize) - 1;
  return { offset, end, length: end - offset + 1 };
}

async function readMetadata(env: Env, name: string, digest: string): Promise<UploadMetadata | null> {
  const obj = await env.REGISTRY.get(metaKey(name, digest));
  if (obj === null) return null;
  try {
    const parsed = uploadMetadataSchema.safeParse(await obj.json());
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}

async function writeMetadata(env: Env, meta: UploadMetadata): Promise<void> {
  await env.REGISTRY.put(metaKey(meta.name, meta.digest), JSON.stringify(meta), {
    httpMetadata: { contentType: "application/json" },
  });
}

// Removes the metadata object and every recorded part object. Best-effort: callers have already
// achieved (or abandoned) the terminal state, so failures here only leave harmless residue that a
// later cron pass will retry.
async function deleteUploadTracking(env: Env, name: string, digest: string): Promise<void> {
  const prefix = partsPrefix(name, digest);
  let cursor: string | undefined;
  do {
    const listed = await env.REGISTRY.list({ prefix, cursor });
    if (listed.objects.length > 0) {
      await env.REGISTRY.delete(listed.objects.map((o) => o.key));
    }
    cursor = listed.truncated ? listed.cursor : undefined;
  } while (cursor !== undefined);
  await env.REGISTRY.delete(metaKey(name, digest));
}

type RecordedPart = { partNumber: number; etag: string; uploaded: number };

// Lists the recorded parts for an upload. ETags are stored in each part object's customMetadata so
// finalize can rebuild the R2UploadedPart[] with a single (paginated) list instead of N gets.
async function listRecordedParts(env: Env, name: string, digest: string): Promise<RecordedPart[]> {
  const prefix = partsPrefix(name, digest);
  const parts: RecordedPart[] = [];
  let cursor: string | undefined;
  do {
    const listed = await env.REGISTRY.list({ prefix, cursor, include: ["customMetadata"] });
    for (const obj of listed.objects) {
      const partNumber = Number(obj.key.slice(prefix.length));
      const etag = obj.customMetadata?.etag;
      if (!Number.isInteger(partNumber) || partNumber <= 0 || etag === undefined) continue;
      parts.push({ partNumber, etag, uploaded: obj.uploaded.getTime() });
    }
    cursor = listed.truncated ? listed.cursor : undefined;
  } while (cursor !== undefined);
  return parts;
}

async function enqueue(env: Env, message: BlobCacheMessage): Promise<void> {
  if (env.BLOB_CACHE_QUEUE === undefined) return;
  await env.BLOB_CACHE_QUEUE.send(message);
}

async function enqueuePartWindow(
  env: Env,
  config: BlobCacheConfig,
  meta: UploadMetadata,
  fromPartNumber: number,
): Promise<void> {
  if (env.BLOB_CACHE_QUEUE === undefined) return;
  const messages: { body: BlobCacheMessage }[] = [];
  for (
    let partNumber = fromPartNumber;
    partNumber < fromPartNumber + config.maxConcurrentParts && partNumber <= meta.totalParts;
    partNumber++
  ) {
    messages.push({
      body: { type: "upload-part", registry: meta.registry, name: meta.name, digest: meta.digest, partNumber },
    });
  }
  if (messages.length > 0) {
    await env.BLOB_CACHE_QUEUE.sendBatch(messages);
  }
}

// Resolves the upstream client for a message, or null if the registry is no longer configured.
function resolveSource(env: Env, registryURL: string): Registry | null {
  const configuration = registries(env).find((registry) => registry.registry === registryURL);
  if (configuration === undefined) return null;
  return new RegistryHTTPClient(env, configuration);
}

// ---- Queue steps -----------------------------------------------------------

// "start": create the multipart upload (once), write the metadata, and kick off the first window of
// part messages. Idempotent under redelivery: if metadata already exists it resumes rather than
// creating a second (orphaned) multipart upload.
export async function startBlobCacheUpload(
  env: Env,
  config: BlobCacheConfig,
  source: Registry,
  msg: BlobCacheMessage,
): Promise<void> {
  const { registry, name, digest } = msg;
  if (!isValidDigest(digest)) {
    console.error(`blob-cache start: invalid digest ${digest} for ${name}, dropping`);
    return;
  }

  // Already cached (by a previous run or the inline path) -> nothing to do.
  const cached = await env.REGISTRY.head(finalKey(name, digest));
  if (cached !== null) {
    console.log(`blob-cache start: ${digest} for ${name} already cached, skipping`);
    return;
  }

  // Resume an existing upload instead of starting a new multipart upload.
  const existing = await readMetadata(env, name, digest);
  if (existing !== null && existing.status === "in-progress") {
    console.log(
      `blob-cache start: resuming ${digest} for ${name} uploadId=${existing.uploadId} totalParts=${existing.totalParts}`,
    );
    await enqueuePartWindow(env, config, existing, 1);
    return;
  }

  const upstream = await source.layerExists(name, digest);
  if ("response" in upstream) {
    throw new Error(`upstream layerExists failed with status ${upstream.response.status}`);
  }
  if (!upstream.exists) {
    console.warn(`blob-cache start: upstream no longer has ${digest} for ${name}, skipping`);
    return;
  }

  const totalSize = upstream.size;
  if (!Number.isInteger(totalSize) || totalSize <= 0) {
    console.error(`blob-cache start: upstream reported invalid size ${totalSize} for ${digest}, dropping`);
    return;
  }

  // Grow the part size if needed so we never exceed R2's 10,000-part ceiling for very large blobs.
  let partSize = config.partSize;
  const minPartSizeForCount = Math.ceil(totalSize / R2_MAX_PARTS);
  if (minPartSizeForCount > partSize) partSize = minPartSizeForCount;
  if (partSize > MAXIMUM_CHUNK) {
    console.error(`blob-cache start: layer ${digest} is too large to cache within R2 limits (${totalSize} bytes)`);
    return;
  }
  const totalParts = Math.ceil(totalSize / partSize);

  const key = finalKey(name, digest);
  // Write the multipart upload straight to the final content-addressable key. It only becomes
  // visible on CompleteMultipartUpload, so a partial upload never exposes a corrupt blob and there
  // is no second full-object copy pass (which is what previously blew the invocation budget).
  const upload = await env.REGISTRY.createMultipartUpload(key);

  // Narrow the concurrent-start race: if another invocation wrote metadata while we were creating
  // our upload, abort ours and resume theirs so we don't orphan a multipart upload.
  const raced = await readMetadata(env, name, digest);
  if (raced !== null && raced.status === "in-progress") {
    await upload.abort().catch(() => {});
    await enqueuePartWindow(env, config, raced, 1);
    return;
  }

  const now = Date.now();
  const meta: UploadMetadata = {
    version: 1,
    registry,
    name,
    digest,
    key,
    uploadId: upload.uploadId,
    totalSize,
    partSize,
    totalParts,
    status: "in-progress",
    createdAt: now,
    updatedAt: now,
  };
  await writeMetadata(env, meta);
  console.log(
    `blob-cache start: ${digest} for ${name} uploadId=${meta.uploadId} totalParts=${totalParts} partSize=${partSize}`,
  );
  await enqueuePartWindow(env, config, meta, 1);
}

// "upload-part": fetch a single byte range from the upstream and upload it as one R2 part. Idempotent
// (skips work if the part is already recorded), then advances the sliding window and asks finalize
// to check for completion.
export async function uploadBlobCachePart(
  env: Env,
  config: BlobCacheConfig,
  source: Registry,
  msg: BlobCacheMessage,
): Promise<void> {
  const { name, digest } = msg;
  const partNumber = msg.partNumber;
  if (partNumber === undefined) {
    console.error(`blob-cache upload-part: missing partNumber for ${digest}, dropping`);
    return;
  }

  const meta = await readMetadata(env, name, digest);
  if (meta === null || meta.status !== "in-progress") {
    // Upload finished or was aborted+restarted; this message is stale.
    return;
  }

  if (partNumber > meta.totalParts) {
    console.error(`blob-cache upload-part: part ${partNumber} out of range for ${digest}, dropping`);
    return;
  }

  // Idempotency: if the part is already recorded, don't re-fetch/re-upload it.
  const alreadyUploaded = (await env.REGISTRY.head(partKey(name, digest, partNumber))) !== null;
  if (!alreadyUploaded) {
    const { offset, end, length } = partRange(meta, partNumber);
    const res = await source.getLayer(name, digest, { offset, end });
    if ("response" in res) {
      throw new Error(`upstream returned status ${res.response.status} for part ${partNumber} of ${digest}`);
    }
    // The upstream must honor the range; a full-body (200) response would corrupt the part layout.
    if (res.contentRange === undefined || res.contentRange.start !== offset || res.contentRange.end !== end) {
      await res.stream.cancel().catch(() => {});
      throw new Error(`upstream did not honor range for part ${partNumber} of ${digest}`);
    }

    const upload = env.REGISTRY.resumeMultipartUpload(meta.key, meta.uploadId);
    const uploaded = await upload.uploadPart(partNumber, limit(res.stream, length));
    // Record the part only after uploadPart succeeds, so a recorded part always corresponds to a
    // real R2 part. ETag lives in customMetadata for cheap listing at finalize time.
    await env.REGISTRY.put(partKey(name, digest, partNumber), "", {
      customMetadata: { etag: uploaded.etag, size: String(length) },
    });
    console.log(
      `blob-cache upload-part: ${digest} for ${name} part=${partNumber}/${meta.totalParts} bytes=${length} outcome=uploaded`,
    );
  } else {
    console.log(
      `blob-cache upload-part: ${digest} for ${name} part=${partNumber}/${meta.totalParts} outcome=skipped-existing`,
    );
  }

  // Advance the sliding window and trigger a completion check. Done after recording so a failing
  // part retries without amplifying the fan-out.
  const next = partNumber + config.maxConcurrentParts;
  if (next <= meta.totalParts) {
    await enqueue(env, { type: "upload-part", registry: meta.registry, name, digest, partNumber: next });
  }
  await enqueue(env, { type: "finalize", registry: meta.registry, name, digest });
}

// "finalize": if every part is recorded, complete the multipart upload and clean up. Safe to run
// many times and concurrently; a lost race just observes the finished object and returns.
export async function finalizeBlobCacheUpload(
  env: Env,
  _config: BlobCacheConfig,
  msg: BlobCacheMessage,
): Promise<void> {
  const { name, digest } = msg;
  const meta = await readMetadata(env, name, digest);
  if (meta === null) return;

  // Already completed by a concurrent finalize (or a prior run): clean up any residue and return.
  if ((await env.REGISTRY.head(meta.key)) !== null) {
    await deleteUploadTracking(env, name, digest);
    return;
  }

  const recorded = await listRecordedParts(env, name, digest);
  if (recorded.length < meta.totalParts) {
    // Not all parts are in yet; a later part's finalize message will complete the upload.
    return;
  }

  recorded.sort((a, b) => a.partNumber - b.partNumber);
  const uploadedParts = recorded.map((part) => ({ partNumber: part.partNumber, etag: part.etag }));

  const upload = env.REGISTRY.resumeMultipartUpload(meta.key, meta.uploadId);
  try {
    await upload.complete(uploadedParts);
  } catch (err) {
    // A concurrent finalize may have completed it first; treat an existing object as success.
    if ((await env.REGISTRY.head(meta.key)) !== null) {
      await deleteUploadTracking(env, name, digest);
      return;
    }
    throw err;
  }

  console.log(
    `blob-cache finalize: ${digest} for ${name} uploadId=${meta.uploadId} parts=${meta.totalParts} outcome=completed`,
  );
  await deleteUploadTracking(env, name, digest);
}

// ---- Batch + cron entrypoints ---------------------------------------------

async function processBlobCacheMessage(env: Env, config: BlobCacheConfig, msg: BlobCacheMessage): Promise<void> {
  if (msg.type === "finalize") {
    await finalizeBlobCacheUpload(env, config, msg);
    return;
  }

  const source = resolveSource(env, msg.registry);
  if (source === null) {
    // The upstream registry is no longer configured; retrying can't help.
    console.warn(`blob-cache: no registry configuration for ${msg.registry}, dropping ${msg.digest}`);
    return;
  }

  if (msg.type === "start") {
    await startBlobCacheUpload(env, config, source, msg);
  } else {
    await uploadBlobCachePart(env, config, source, msg);
  }
}

export async function handleBlobCacheBatch(batch: MessageBatch<unknown>, env: Env): Promise<void> {
  const config = blobCacheConfig(env);
  for (const message of batch.messages) {
    const parsed = blobCacheMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      // A malformed message can never succeed, so drop it instead of retrying forever.
      console.error("Dropping invalid blob cache message:", errorString(parsed.error));
      message.ack();
      continue;
    }

    try {
      await processBlobCacheMessage(env, config, parsed.data);
      message.ack();
    } catch (err) {
      console.error(
        `blob-cache: failed to process ${parsed.data.type} for ${parsed.data.digest}, will retry:`,
        errorString(err),
      );
      message.retry();
    }
  }
}

// Cron cleanup: walks every tracked upload and either finishes it, abandons+restarts it, or leaves
// it alone if it is still making progress. This is what guarantees no multipart upload is orphaned
// forever the way the previous single-shot design left them.
export async function cleanupBlobCacheUploads(env: Env, config: BlobCacheConfig): Promise<void> {
  const now = Date.now();
  let cursor: string | undefined;
  do {
    const listed = await env.REGISTRY.list({ prefix: `${CACHE_PREFIX}/`, cursor });
    for (const obj of listed.objects) {
      if (!obj.key.endsWith("/meta.json")) continue;
      try {
        await cleanupUpload(env, config, obj.key, now);
      } catch (err) {
        console.error(`blob-cache cleanup: error handling ${obj.key}:`, errorString(err));
      }
    }
    cursor = listed.truncated ? listed.cursor : undefined;
  } while (cursor !== undefined);
}

async function cleanupUpload(env: Env, config: BlobCacheConfig, key: string, now: number): Promise<void> {
  const obj = await env.REGISTRY.get(key);
  if (obj === null) return;
  const parsed = uploadMetadataSchema.safeParse(await obj.json().catch(() => null));
  if (!parsed.success) {
    // Unparseable metadata is unrecoverable; drop it so it stops being listed.
    await env.REGISTRY.delete(key);
    return;
  }

  const meta = parsed.data;
  const { name, digest } = meta;

  // Already finished: the completed object exists, just clear the tracking residue.
  if ((await env.REGISTRY.head(meta.key)) !== null) {
    await deleteUploadTracking(env, name, digest);
    return;
  }

  const recorded = await listRecordedParts(env, name, digest);

  // All parts uploaded but never completed (a lost finalize message): nudge finalize instead of
  // throwing away a fully-uploaded blob.
  if (recorded.length >= meta.totalParts && meta.totalParts > 0) {
    await enqueue(env, { type: "finalize", registry: meta.registry, name, digest });
    return;
  }

  // Progress signal: the most recent part activity, falling back to when the upload was created.
  const lastActivity = recorded.reduce((max, part) => Math.max(max, part.uploaded), meta.updatedAt);
  if (now - lastActivity <= config.staleAbortMs) {
    // Still making progress; leave it alone.
    return;
  }

  // Stalled: abort the multipart upload (releasing its stored parts), delete tracking, and restart
  // from scratch so caching eventually succeeds.
  console.warn(
    `blob-cache cleanup: aborting stalled upload ${digest} for ${name} uploadId=${meta.uploadId} recorded=${recorded.length}/${meta.totalParts}`,
  );
  await env.REGISTRY.resumeMultipartUpload(meta.key, meta.uploadId)
    .abort()
    .catch(() => {});
  await deleteUploadTracking(env, name, digest);
  await enqueue(env, { type: "start", registry: meta.registry, name, digest });
}

export async function handleBlobCacheCleanup(env: Env): Promise<void> {
  await cleanupBlobCacheUploads(env, blobCacheConfig(env));
}
