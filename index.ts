/**
 * The core server that runs on a Cloudflare worker.
 */

import { Router } from "itty-router";
import { AuthErrorResponse, InternalError } from "./src/errors";
import v2Router from "./src/router";
import { authenticationMethodFromEnv } from "./src/authentication-method";
import { Registry } from "./src/registry/registry";
import { R2Registry } from "./src/registry/r2";
import {
  BlobCacheMessage,
  handleBlobCacheBatch,
  handleBlobCacheCleanup,
  blobCacheQueueName,
} from "./src/registry/cache";

// A full compatibility mode means that the r2 registry will try its best to
// help the client on the layer push. See how we let the client push layers with chunked uploads for more information.
type PushCompatibilityMode = "full" | "none";

export interface Env {
  REGISTRY: R2Bucket;
  ENVIRONMENT: string;
  JWT_REGISTRY_TOKENS_PUBLIC_KEY?: string;
  USERNAME?: string;
  PASSWORD?: string;
  READONLY_USERNAME?: string;
  READONLY_PASSWORD?: string;
  PUSH_COMPATIBILITY_MODE?: PushCompatibilityMode;
  REGISTRIES_JSON?: string; // should be in the format of RegistryConfiguration[];
  REGISTRY_CLIENT: Registry;
  // Optional queue that receives background cache jobs for pull-through layers too big to cache inline.
  BLOB_CACHE_QUEUE?: Queue<BlobCacheMessage>;
  // Optional override for the blob cache queue name; defaults to "blob-cache".
  BLOB_CACHE_QUEUE_NAME?: string;
  // Optional tuning knobs for the background blob cache (all numeric strings). See src/registry/cache.ts.
  // Target size in bytes for each multipart part (default 256MiB, clamped to R2's 5MiB..5GiB range).
  BLOB_CACHE_PART_SIZE?: string;
  // How long (ms) a tracked upload may make no progress before the cron cleanup aborts+restarts it (default 5min).
  BLOB_CACHE_STALE_ABORT_MS?: string;
  // Sliding-window size: how many "upload-part" messages are kept in flight per blob (default 6).
  BLOB_CACHE_MAX_CONCURRENT_PARTS?: string;
}

const router = Router();

/**
 * V2 Api
 */
router.all("/v2/*", v2Router.fetch);

router.all("*", () => new Response("Not Found.", { status: 404 }));

export default {
  async fetch(request: Request, env: Env, context?: ExecutionContext) {
    if (!ensureConfig(env)) {
      return new AuthErrorResponse(request);
    }

    const authMethod = await authenticationMethodFromEnv(env);
    if (!authMethod) {
      return new AuthErrorResponse(request);
    }

    const credentials = await authMethod.checkCredentials(request);
    if (!credentials.verified) {
      console.warn(`Not Authorized. authmode=${authMethod.authmode}. verified=false`);
      return new AuthErrorResponse(request);
    }

    env.REGISTRY_CLIENT = new R2Registry(env);
    try {
      // Dispatch the request to the appropriate route
      const res = await router.fetch(request, env, context);
      return res;
    } catch (err) {
      if (err instanceof Response) {
        console.warn(`${request.method} ${err.status} ${err.url}`);
        return err;
      }

      // Unexpected error
      if (err instanceof Error) {
        console.error(
          "An error has been thrown by the router:\n",
          `${err.name}: ${err.message}: ${err.cause}: ${err.stack}`,
        );
        return new InternalError();
      }

      console.error(
        "An error has been thrown and is neither a Response or an Error, JSON.stringify() =",
        JSON.stringify(err),
      );
      return new InternalError();
    }
  },

  async queue(batch: MessageBatch<unknown>, env: Env): Promise<void> {
    if (batch.queue === blobCacheQueueName(env)) {
      await handleBlobCacheBatch(batch, env);
      return;
    }

    console.error(`Received a batch from an unhandled queue: ${batch.queue}`);
    batch.retryAll();
  },

  // Cron-triggered cleanup of the background blob cache: completes, restarts, or leaves alone every
  // tracked multipart upload so large layers eventually finish caching and stalled uploads are never
  // orphaned. No-op when no blob cache uploads are in flight.
  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      handleBlobCacheCleanup(env).catch((err) => {
        console.error("blob-cache cleanup failed:", err);
      }),
    );
  },
} satisfies ExportedHandler<Env>;

const ensureConfig = (env: Env): boolean => {
  if (!env.REGISTRY) {
    console.error(
      "env.REGISTRY is not setup. Please setup an R2 bucket and add the binding in your wrangler config file. Try 'npx wrangler --env production r2 bucket create r2-registry'",
    );
    return false;
  }

  return true;
};
