# AGENTS.md

## Cursor Cloud specific instructions

This repo is `serverless-registry`: a container registry that runs on a Cloudflare
Worker (`index.ts` + `src/`) and stores blobs/manifests in an R2 bucket. It implements the
Docker Registry V2 HTTP API. The `push/` folder is a separate pnpm workspace package (a helper
CLI for pushing very large layers). See `README.md` and `CONTRIBUTING.md` for product/deploy details.

### Node version (important, non-obvious)

- This project requires **Node >= 24** (see `engines` in `package.json`; CI uses Node 24).
- The default `node` on `PATH` in this VM is **v22** (`/exec-daemon/node`), which shadows nvm even
  though nvm's default alias is 24. `.bashrc` cannot win this because `/exec-daemon` is prepended
  after profile files load.
- Activate Node 24 at the start of any shell before running `pnpm`/`wrangler`/`vitest`/`tsc`:
  - `nvm use 24` (preferred), or
  - `export PATH="$HOME/.nvm/versions/node/v24.19.0/bin:$PATH"`
- `pnpm install` itself still succeeds under Node 22 (it only warns), so the startup update script
  does not need Node 24 — but running/testing the Worker does.

### Package manager

- Use **pnpm** (v11; `packageManager` is pinned). It is an nvm-managed binary, so it is only on
  `PATH` once nvm is sourced (a normal login shell does this).

### Standard commands

Run these from the repo root (defined in `package.json` scripts):

- Lint: `pnpm run lint`
- Format check: `pnpm run format:check`
- Typecheck: `pnpm run typecheck` — note this runs `wrangler types` and regenerates the committed
  `worker-configuration.d.ts`; it normally regenerates identically (clean git status).
- Tests: `pnpm test` (vitest via `@cloudflare/vitest-pool-workers` / miniflare).

### Test caveat (slow VM)

- Two `background layer caching` tests exercise a simulated >5GiB multipart upload and can exceed
  vitest's default 5s `testTimeout` on a slower VM, showing up as `Test timed out in 5000ms`.
  They are **not** code failures — they pass with a larger timeout:
  `npx vitest run --testTimeout=60000 -t "background layer caching"`.

### Running the Worker locally (dev)

- `wrangler.jsonc` / `wrangler.toml` are **gitignored**, so no config exists on a fresh checkout.
  Create one first: `cp wrangler.example.jsonc wrangler.jsonc`.
- Start dev server: `pnpm run dev:miniflare` (port 9999) or `wrangler dev --env dev --port 8787`.
- Local dev needs **no Cloudflare account**: it uses a local (miniflare) R2 bucket and the built-in
  dev credentials `USERNAME=hello` / `PASSWORD=world`. Requests without valid credentials get `401`.

### Testing the registry without Docker

- Docker is **not** installed in this VM. Exercise the registry directly over its Docker Registry V2
  HTTP API with `curl` and basic auth `hello:world` (blob upload = `POST` then `PATCH` then
  `PUT ...&digest=sha256:<hash>`, then `PUT /v2/<name>/manifests/<tag>`, then pull it back).
