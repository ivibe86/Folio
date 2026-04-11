Folio — Docker Infrastructure Documentation


docker-compose.yml

Purpose

This file is the top-level orchestration manifest for the Folio application stack. It defines two services — backend (FastAPI + SQLite) and frontend (SvelteKit) — along with their build contexts, port bindings, volume mounts, environment variable injection, health-check dependencies, and restart policies. It is the single entry point for spinning up the entire application locally or in a self-hosted deployment with a single docker compose up --build command.


Key Dependencies

Docker Compose v3+ syntax — uses services, build.args, env_file, depends_on.condition, and restart directives.
.env file — consumed by env_file on the backend and implicitly by Compose itself for variable interpolation (e.g., ${BACKEND_PORT:-8000}).
./backend/Dockerfile and ./frontend/Dockerfile — the build instructions for each service image.
./data/ volume — SQLite database persistence directory mounted into the backend container.
./certs/ volume — TLS certificate directory for Teller API mTLS authentication, mounted read-only into the backend.

Core Functions / Classes / Exports

backend service

What it does: Builds and runs the FastAPI application server.
Inputs: Build context ./backend, .env file, ./data and ./certs host directories.
Outputs / Side effects: Exposes port 8000 (configurable via BACKEND_PORT), writes the SQLite database to /data/finflow.db, reads Teller mTLS certs from /certs/.
Notable logic:
Uses ${BACKEND_PORT:-8000}:8000 syntax, meaning the host port is configurable but the container port is always 8000. This is important: the backend Uvicorn process always listens on 8000 regardless of what host port is chosen.
./certs is mounted :ro (read-only), which is a good security practice — the backend can read certificates but cannot write or modify them.
DB_FILE=/data/finflow.db is injected as an environment variable rather than hardcoded inside the application image, allowing the path to be overridden without a rebuild.
SSL_CERT_FILE and REQUESTS_CA_BUNDLE both default to the standard Debian CA bundle path, ensuring that Python's requests library and the standard SSL module both trust the system CA store (important for Teller API HTTPS calls).
restart: unless-stopped means the container will auto-restart on failure or Docker daemon restart, but not if manually stopped.

frontend service

What it does: Builds and runs the SvelteKit Node.js adapter server.
Inputs: Build context ./frontend, build-time ARGs (VITE_API_KEY, VITE_TELLER_APP_ID, VITE_TELLER_ENVIRONMENT), runtime environment variables (ORIGIN, BACKEND_URL).
Outputs / Side effects: Exposes port 3000 (configurable via FRONTEND_PORT), proxies API calls to http://backend:8000 via Docker's internal DNS.
Notable logic:
VITE_* variables are passed as build arguments, not runtime environment variables. This is architecturally significant: they are baked into the compiled JavaScript bundle at build time by Vite. Changing them requires a full image rebuild, not just a container restart.
ORIGIN=http://localhost:${FRONTEND_PORT:-3000} is a SvelteKit-specific requirement for CSRF protection. It must match the URL the browser uses to access the app. This could break in production if the app is served behind a reverse proxy at a different URL.
BACKEND_URL=http://backend:8000 uses Docker Compose's automatic DNS resolution — backend resolves to the backend container's internal IP within the Compose network.
depends_on: backend: condition: service_healthy ensures the frontend container does not start until the backend passes its health check. This prevents race conditions where the frontend might attempt API calls before the backend is ready.

Data Flow

Developer runs docker compose up --build.
Compose reads .env for variable interpolation and injects it into the backend via env_file.
Backend image is built from ./backend/Dockerfile, then started. The /data and /certs host directories are mounted in.
Backend health check (/healthz/health) is polled every 30s until it passes.
Once healthy, the frontend image is built (with VITE_* args baked in) and started.
Browser connects to http://localhost:3000 → SvelteKit server → proxies API requests to http://backend:8000 over Docker's internal network.

Integration Points

Calls: ./backend/Dockerfile, ./frontend/Dockerfile at build time.
References: .env for secrets and port configuration.
Consumed by: Docker Compose CLI, CI/CD pipelines, or any docker compose invocation.
Exposes to host: Ports 3000 (frontend) and 8000 (backend), both configurable.

Known Quirks / Design Notes

VITE_* vars are build-time only. If a developer changes VITE_API_KEY in .env and runs docker compose up without --build, the change will not take effect. This is a common source of confusion and could benefit from a comment in the file or a Makefile target that enforces --build when .env changes.
No named Docker network defined. Compose creates a default bridge network automatically, which works fine but is invisible in the config. Explicitly declaring a named network (e.g., folio-net) would improve clarity and make it easier to attach additional services (e.g., a future Redis cache or monitoring sidecar).
No production HTTPS. ORIGIN is hardcoded to http:// (not https://). Deploying this behind a TLS-terminating reverse proxy (e.g., Caddy, Nginx) would require updating ORIGIN to the public HTTPS URL.
SQLite volume path ./data is relative to the docker-compose.yml location. This is fine locally but could be fragile in CI environments where the working directory may differ.
No resource limits (mem_limit, cpus) are defined. For a production deployment on a shared host, these should be added to prevent runaway processes.


backend/Dockerfile

Purpose

This Dockerfile defines the build and runtime environment for the Folio FastAPI backend. It produces a minimal Python 3.12 image that installs system CA certificates, Python dependencies, and the application source code, then runs the Uvicorn ASGI server. It also includes a built-in HTTP health check endpoint used by Docker Compose to gate frontend startup.


Key Dependencies

python:3.12-slim — base image; Debian-based slim variant balances small size with compatibility (importantly, it has apt-get for installing ca-certificates).
ca-certificates (apt package) — system-level CA bundle; required so Python's ssl module and requests library can validate HTTPS connections to external APIs (Teller, Anthropic/Claude).
requirements.txt — Python package manifest; defines all FastAPI, SQLite, Teller, and Claude dependencies.
uvicorn — ASGI server that serves the FastAPI app object exported from main.py.

Core Functions / Classes / Exports

Build stage: CA certificate installation

What it does: Runs apt-get install ca-certificates and update-ca-certificates before any application code is copied in.
Notable logic: This step is placed early (before COPY requirements.txt) so it is not invalidated by dependency changes. However, it is invalidated by base image updates. The rm -rf /var/lib/apt/lists/* cleanup reduces the image layer size by removing the apt package index after installation.

Build stage: Dependency installation

What it does: Copies requirements.txt first and runs pip install before copying the rest of the application source.
Notable logic: This is the classic layer caching optimization — because requirements.txt changes less frequently than application code, Docker can reuse the pip install layer on rebuilds where only source files changed. This significantly speeds up iterative development.

Build stage: .env removal

What it does: RUN rm -f .env .env_bkp deletes any .env or backup env files that might have been copied into the image.
Notable logic: The .dockerignore file should already prevent .env from being included in the build context, but this RUN step acts as a belt-and-suspenders security measure. If .dockerignore is misconfigured or bypassed, secrets will not be baked into the image. This is an important defense-in-depth practice.

Runtime: Health check

What it does: Polls http://localhost:8000/healthz/health every 30 seconds using Python's built-in urllib.request.
Inputs: No external input; relies on the Uvicorn server being up and the /healthz/health route returning a 2xx response.
Outputs / Side effects: Returns exit code 0 (healthy) or 1 (unhealthy). Docker Compose's depends_on: condition: service_healthy reads this status.
Notable logic: Uses urllib.request rather than curl or wget to avoid installing additional system packages. The --start-period=10s gives Uvicorn 10 seconds to initialize before health checks begin — important if the app performs database migrations or schema setup on startup.

Runtime: CMD

What it does: Starts the FastAPI application via uvicorn main:app --host 0.0.0.0 --port 8000.
Notable logic: --host 0.0.0.0 is required inside Docker — binding to 127.0.0.1 (the default) would make the service unreachable from outside the container. No --workers flag is set, so Uvicorn runs in single-process mode. For production, this could be replaced with gunicorn using uvicorn.workers.UvicornWorker for multi-process concurrency.

Data Flow

Docker builds the image: installs OS deps → installs Python deps → copies source → removes .env.
Container starts: Uvicorn loads main.py, initializes the FastAPI app (likely including SQLite schema setup).
Docker polls /healthz/health; once passing, the frontend container is allowed to start.
At runtime, the backend reads DB_FILE, TELLER_CERT_PATH, TELLER_KEY_PATH, and CA bundle paths from environment variables injected by docker-compose.yml.

Integration Points

Built by: docker-compose.yml backend.build directive.
Exposes: Port 8000 to the Docker Compose network (consumed by frontend via http://backend:8000).
Reads from host (via volumes): ./data/finflow.db (SQLite), ./certs/teller-cert.pem, ./certs/teller-key.pem.
Calls externally: Teller API (mTLS), Anthropic Claude API (HTTPS) — both require the CA bundle installed in this Dockerfile.

Known Quirks / Design Notes

Single-process Uvicorn — no --workers flag means the backend is single-threaded for CPU-bound work. For Claude categorization (which involves network I/O), Python's asyncio in FastAPI handles concurrency well, but if synchronous blocking calls exist in the codebase, this could become a bottleneck.
No non-root user — the container runs as root by default. A security best practice would be to add RUN useradd -m appuser && chown -R appuser /app /data and USER appuser before the CMD. This limits the blast radius if the container is compromised.
/data directory created in image — RUN mkdir -p /data creates the directory inside the image, but this is overridden by the volume mount at runtime. It exists as a fallback for running the container without the volume (e.g., during testing), though data would not persist.
requirements.txt is not shown — the exact Python dependencies are not visible in these Docker files. Future documentation should include a breakdown of requirements.txt.
Health check uses HTTP, not HTTPS — the health check calls http://localhost:8000/healthz/health. If the backend is ever configured to enforce HTTPS internally, this check would break.


.dockerignore

Purpose

This file instructs the Docker build daemon on which files and directories to exclude from the build context sent to the Docker engine during docker build. It applies to both the backend and frontend build contexts. Its primary goals are: (1) preventing secrets (.env files) from being accidentally baked into images, (2) reducing build context size by excluding large directories like node_modules and .venv, and (3) avoiding cache invalidation from files that don't affect the build (e.g., IDE configs, OS artifacts).


Key Dependencies

Docker build context mechanism — .dockerignore is processed by the Docker CLI before sending files to the daemon. Patterns follow .gitignore syntax.
** glob prefix — used extensively (e.g., **/.env) to match files at any depth in the directory tree, ensuring exclusions apply regardless of nesting level.

Core Functions / Classes / Exports

This file has no functions or classes; it is a configuration file. Its significant entries are documented below.


.env / .env_bkp / .env.local exclusion (**/.env, **/.env_bkp, **/.env.local)

What it does: Prevents any environment files at any directory depth from entering the build context.
Notable logic: This is the primary security control for secret management in this project. Combined with the RUN rm -f .env .env_bkp step in the backend Dockerfile, there are two independent layers of protection. The ** prefix is critical — without it, a .env file in a subdirectory (e.g., ./backend/.env) could slip through.

data/ exclusion

What it does: Excludes the SQLite database directory from the build context.
Notable logic: The data/ directory can grow arbitrarily large as the SQLite database accumulates transaction records. Including it in the build context would slow down every rebuild. It is correctly provided at runtime via a Docker volume mount instead.

**/node_modules exclusion

What it does: Excludes Node.js package directories.
Notable logic: node_modules can contain tens of thousands of files and hundreds of megabytes. The frontend Dockerfile runs npm ci inside the container to install dependencies fresh, so the host's node_modules is never needed. Including it would massively bloat the build context and potentially introduce platform-incompatible native modules (e.g., binaries compiled for macOS being copied into a Linux container).

**/.venv / **/__pycache__ / **/*.pyc exclusion

What it does: Excludes Python virtual environments and compiled bytecode.
Notable logic: Similar rationale to node_modules — the backend Dockerfile runs pip install inside the container. Host .venv directories may contain platform-specific compiled extensions. __pycache__ and .pyc files are regenerated by Python inside the container and would cause unnecessary cache busting if included.

**/build / **/.svelte-kit exclusion

What it does: Excludes SvelteKit build artifacts and the .svelte-kit internal directory.
Notable logic: The frontend Dockerfile runs npm run build inside the container via a multi-stage build. Host-side build artifacts should never be copied in — they may be stale, platform-incompatible, or built with different environment variables. Excluding .svelte-kit also prevents the Vite dev-server cache from polluting the build context.

.git exclusion

What it does: Excludes the Git repository metadata.
Notable logic: .git directories can be large (especially with long history) and contain no information needed at runtime. Without this exclusion, every commit would invalidate the entire Docker build cache.

Data Flow

The .dockerignore file is a negative filter applied at build-context construction time. When docker compose build is run:


Docker CLI walks the build context directory (./backend or ./frontend).
For each file, it checks .dockerignore patterns.
Matched files are excluded from the tarball sent to the Docker daemon.
The Dockerfile's COPY instructions only have access to the filtered set of files.

Integration Points

Applied during: docker compose build for both backend and frontend services.
Works with: Backend Dockerfile's RUN rm -f .env .env_bkp (belt-and-suspenders secret protection).
Affects: Build speed, image security, and Docker layer cache efficiency.

Known Quirks / Design Notes

Single .dockerignore at repo root — this file appears to live at the repository root and applies broadly. Docker actually looks for .dockerignore relative to the build context path specified in docker-compose.yml. If context: ./backend is set, Docker looks for ./backend/.dockerignore. It's worth verifying whether this root-level file is being picked up correctly for both build contexts, or whether per-service .dockerignore files in ./backend/ and ./frontend/ would be more explicit and reliable.
README.md exclusion — documentation is excluded from the image, which is correct for production images. However, if any runtime code attempts to read README.md (unlikely but possible for version/info endpoints), it would silently fail inside the container.
No exclusion of test files — test directories (e.g., tests/, __tests__/, *.test.py, *.spec.ts) are not explicitly excluded. These could be added to reduce image size and prevent test code from being present in production containers.
Thumbs.db / .DS_Store — these OS-generated files are correctly excluded, indicating the project is developed across both macOS and Windows environments.


frontend/Dockerfile

Purpose

This Dockerfile implements a multi-stage build for the SvelteKit frontend. The first stage (builder) installs Node.js dependencies and compiles the SvelteKit application into a production bundle using Vite, with VITE_* environment variables baked in at compile time. The second stage produces a lean production image containing only the compiled output and runtime Node.js dependencies, then serves the app using SvelteKit's Node adapter. This approach minimizes the final image size by discarding build tooling (Vite, TypeScript compiler, etc.) from the production image.


Key Dependencies

node:20-slim — Node.js 20 LTS slim image used for both build and production stages; Debian-based, minimal footprint.
npm ci / npm install — installs exact dependency versions from package-lock.json (ci) with fallback to npm install if no lockfile exists.
npm run build — invokes the SvelteKit/Vite build pipeline, compiling TypeScript, processing Svelte components, and bundling assets.
SvelteKit Node adapter — the build/ output directory and node build CMD indicate the project uses @sveltejs/adapter-node, which produces a self-contained Node.js HTTP server.
VITE_* build arguments — Vite's convention for embedding environment variables into client-side bundles at build time.

Core Functions / Classes / Exports

Build stage: ARG / ENV for VITE_* variables

What it does: Declares VITE_API_KEY, VITE_TELLER_APP_ID, and VITE_TELLER_ENVIRONMENT as build arguments, then immediately assigns them to ENV variables so the Vite build process can read them.
Inputs: Values passed from docker-compose.yml build.args.
Notable logic: Docker ARG values are available only during the build stage. By copying them to ENV, they become accessible to RUN npm run build via process.env. This is the standard pattern for injecting Vite build-time variables. Critically: these values will be embedded in the compiled JavaScript and visible in browser dev tools. They should never include server-side secrets.

Build stage: Layer caching optimization

What it does: Copies package.json and package-lock.json before copying the rest of the source, then runs npm ci.
Notable logic: Same layer caching rationale as the backend Dockerfile — node_modules installation is expensive (~seconds to minutes), so isolating it from source code changes means a source-only change reuses the cached npm ci layer. The || npm install fallback handles the edge case where package-lock.json doesn't exist (e.g., a fresh clone without running npm install locally).

Build stage: ENV DOCKER=true

What it does: Sets a DOCKER environment variable to "true" before running the build.
Notable logic: This flag is likely read by svelte.config.js or vite.config.ts to switch behavior when building inside Docker vs. locally. Common use cases include: disabling certain plugins, adjusting asset base paths, or enabling the Node adapter specifically for Docker deployments. The exact effect depends on the frontend source code (not provided), but this is a deliberate escape hatch for environment-specific build logic.

Production stage: Selective COPY --from=builder

What it does: Copies only build/, package.json, and node_modules/ from the builder stage into the final image.
Notable logic: The source code (src/, svelte.config.js, vite.config.ts, etc.) is not copied into the production image. Only the compiled output exists at runtime. This means: (a) smaller image, (b) no source code exposure, (c) no way to npm run dev inside the production container.

Runtime: Environment variables

What it does: Sets PORT=3000, HOST=0.0.0.0, ORIGIN=http://localhost:3000, and BACKEND_URL=http://backend:8000 as defaults; these are overridden by docker-compose.yml at runtime.
Notable logic:
HOST=0.0.0.0 — like the backend, required for Docker networking.
ORIGIN — SvelteKit uses this for CSRF token validation. The value in the Dockerfile (http://localhost:3000) is overridden by docker-compose.yml, but having a default prevents crashes if the container is run standalone without Compose.
BACKEND_URL=http://backend:8000 — used by SvelteKit server-side load functions or API route handlers to proxy requests to the FastAPI backend. This works via Docker DNS within the Compose network.

Runtime: Health check

What it does: Uses Node.js's built-in fetch API (available natively since Node 18) to GET http://localhost:3000 and checks for a successful HTTP response.
Notable logic: Unlike the backend health check (which targets a dedicated /healthz/health endpoint), the frontend health check hits the root route /. This means a successful health check only confirms the Node server is responding — it does not verify backend connectivity or specific route functionality. --start-period=5s is shorter than the backend's 10s, appropriate since the SvelteKit Node server starts very quickly.

Runtime: CMD ["node", "build"]

What it does: Starts the SvelteKit production server by executing the compiled build/index.js entry point via node build.
Notable logic: This is the @sveltejs/adapter-node standard startup command. The build/ directory contains a complete self-hosted HTTP server — no external web server (Apache, Nginx) is needed.

Data Flow

docker compose build frontend triggers the builder stage.
VITE_* build args are passed in from .env via docker-compose.yml.
npm ci installs all dependencies (including devDependencies needed for the Vite build).
npm run build compiles Svelte components, TypeScript, and assets into build/.
Production stage copies only the runtime artifacts.
At runtime, the Node server handles incoming HTTP requests from the browser.
Server-side SvelteKit routes/load functions that need backend data make HTTP calls to http://backend:8000 (Docker internal DNS).
Client-side code uses VITE_API_KEY and VITE_TELLER_APP_ID (baked into JS bundle) for Teller Connect widget initialization.

Integration Points

Built by: docker-compose.yml frontend.build directive.
Depends on: backend service (via depends_on: condition: service_healthy).
Calls: http://backend:8000 for API data (server-side); Teller Connect JavaScript widget (client-side, using baked-in VITE_TELLER_APP_ID).
Exposed to: Browser clients on port 3000.

Known Quirks / Design Notes

VITE_* secrets in browser bundle — VITE_API_KEY is baked into the JavaScript bundle and will be visible to any user who inspects the page source or network traffic. Ensure this key has minimal permissions and is not a server-side secret.
No non-root user — same concern as the backend: the container runs as root. Adding a node user (which node:20-slim includes by default as UID 1000) with USER node would improve security posture.
npm ci || npm install fallback — the || npm install is pragmatic but slightly risky: npm install without a lockfile can install non-deterministic dependency versions. In a CI/CD pipeline, it may be better to fail hard if package-lock.json is missing (npm ci alone) to enforce lockfile discipline.
node_modules copied from builder to production — this includes all devDependencies unless the project uses npm prune --production before copying. For a smaller production image, adding RUN npm prune --production at the end of the builder stage (after the build) would exclude dev-only packages from the final image.
Health check hits / not a dedicated endpoint — if the root route has complex logic (authentication redirects, DB calls via server-side load), a failed root route could produce a non-2xx response and mark the container unhealthy incorrectly. A dedicated GET /healthz endpoint in the SvelteKit app would be more robust.
BACKEND_URL is runtime-only — unlike VITE_* vars, BACKEND_URL is a runtime environment variable and can be changed without rebuilding the image. This is the correct design for server-side configuration.


System-Level Summary

Infrastructure Architecture Overview

Folio's Docker infrastructure implements a clean two-tier web application pattern: a SvelteKit frontend serving browser clients and a FastAPI backend handling business logic, data persistence, and third-party API integrations. These two services are orchestrated by Docker Compose, which manages their build processes, networking, environment variable injection, startup ordering, and restart policies. The entire stack is designed to run with a single docker compose up --build command, making local development and self-hosted deployment straightforward.


Build-Time vs. Runtime Configuration

A key architectural theme across these files is the deliberate separation of build-time and runtime configuration. The frontend requires VITE_* variables at build time because Vite statically inlines them into the JavaScript bundle — these control the Teller Connect widget's app ID and environment. The backend, by contrast, reads all its configuration (database path, certificate paths, CA bundle paths) from runtime environment variables injected by Docker Compose via env_file. This means backend configuration can be changed by editing .env and restarting the container, while frontend configuration changes require a full image rebuild. This distinction is critically important for operators and should be clearly communicated in the project's README.


Security Layering

The infrastructure shows evidence of thoughtful security design through multiple complementary controls. The .dockerignore file prevents .env secrets from entering build contexts; the backend Dockerfile adds a redundant RUN rm -f .env as a belt-and-suspenders measure. Teller mTLS certificates are mounted read-only (./certs:/certs:ro), preventing the backend process from modifying them. The CA certificate installation in the backend image ensures that all outbound HTTPS calls (to Teller and the Claude/Anthropic API) are properly validated against trusted certificate authorities rather than skipping TLS verification. The primary remaining security gaps are the absence of non-root user configuration in both Dockerfiles and the fact that VITE_API_KEY is client-side visible in the browser bundle.


Networking and Service Communication

All inter-service communication happens over Docker Compose's automatically created bridge network. The frontend reaches the backend via http://backend:8000, where backend is resolved by Docker's internal DNS to the backend container's IP address. This means the backend is never directly exposed to the browser — the SvelteKit server acts as a proxy/middleware layer, which is both a security benefit (API keys used server-side never reach the browser) and an architectural pattern that supports future server-side rendering or data aggregation. The host machine exposes ports 3000 (frontend) and 8000 (backend), both configurable via environment variables, giving operators flexibility without requiring Dockerfile changes.


Data Persistence and Stateful Components

Folio uses SQLite as its database, stored at ./data/finflow.db on the host and mounted into the backend container at /data/finflow.db. This volume mount is the only stateful component in the stack — everything else is ephemeral and rebuilt from source. This design is pragmatic for a personal finance app (low concurrent write load, single user or small household), but SQLite's file-locking model means the backend must remain single-process (which it currently is via single-worker Uvicorn). The Teller mTLS certificates in ./certs/ are also host-mounted, making them easy to rotate without a rebuild. For future scaling considerations, migrating from SQLite to PostgreSQL (running as a third Compose service) would be the natural evolution path, with relatively minimal changes to the docker-compose.yml and backend environment configuration.


