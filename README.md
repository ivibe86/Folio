# Folio

Folio is a self-hosted personal finance app that connects to real bank accounts through Teller, enriches merchants, categorizes transactions, and keeps the data on your machine.

The app is still Docker-packaged, but Local AI now runs through a native host Ollama install on macOS and Windows rather than an extra Docker sidecar.

## What Folio Does

- Connects to supported US banks through [Teller](https://teller.io)
- Stores data locally in SQLite
- Enriches raw transaction descriptions into merchant names and industries
- Categorizes transactions with rules, merchant memory, and optional AI
- Includes a finance copilot for natural-language questions
- Supports multiple profiles in one household

## AI Modes

Folio supports three installation modes:

1. `Local AI (Recommended)`
   Folio uses Ollama running natively on your Mac or Windows machine.
2. `Cloud AI (BYOK)`
   Folio uses your external API key for AI features.
3. `No AI`
   Folio runs with deterministic logic and manual categorization only.

## Supported Onboarding

The main setup path is:

- macOS or Windows
- Docker Desktop installed
- `python3 setup.py`

The setup script now guides you through:

- choosing Docker or local development runtime
- choosing `Local AI`, `Cloud AI`, or `No AI`
- Teller certificate setup plus account-linking choice
- Ollama model selection for Local AI
- hardware-aware model recommendation based on your system
- `.env` generation
- starting Docker containers

## Prerequisites

Required:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) for the recommended runtime
- Python 3.11+ to run `setup.py`
- A [Teller](https://teller.io) account with mTLS certificates
- A Teller Application ID for the recommended UI-based Teller Connect flow

Optional:

- [Ollama](https://ollama.com/download) for `Local AI`
- [Anthropic API key](https://console.anthropic.com/) for `Cloud AI`
- [Trove API key](https://trove.headline.com/) if you want optional cloud merchant enrichment

Notes:

- On macOS and Windows, `Local AI` expects Ollama to run on the host OS.
- If you just installed Docker Desktop or Ollama, you may need to relaunch the app, reopen your terminal, or restart the system before setup completes cleanly.

## Quick Start

```bash
git clone https://github.com/yourusername/Folio.git
cd Folio
python3 setup.py
```

The setup script will then walk you through one of these paths:

### Mode A: Local AI

- checks for Ollama
- helps install or launch it
- lets you choose a model preset
- pulls the selected model(s)
- writes `.env`
- starts Docker

### Mode B: Cloud AI

- asks for your Anthropic API key
- optionally enables Trove
- writes `.env`
- starts Docker

### Mode C: No AI

- disables AI features
- writes `.env`
- starts Docker

## Recommended Teller Setup

Folio’s recommended bank-linking flow uses Teller Connect in the UI.

Why this is the recommended path:

- easier setup than manually managing Teller access tokens
- Folio does not store your bank username or password from that flow
- the resulting Teller access tokens are encrypted before storage in the database
- the guided setup defaults Teller Connect to the `development` environment

Folio also supports an advanced fallback path where you add Teller access tokens manually in setup or `.env`, but that path is mainly for users who prefer managing tokens themselves.

## Local AI Model Presets

The installer offers curated presets instead of a huge model list:

- `Light`
  Fastest startup and lowest disk/RAM footprint
- `Balanced`
  Best default for enrichment and categorization on laptops
- `Best Quality`
  Higher-quality copilot responses, higher disk/RAM needs

During setup, Folio detects available system memory, CPU thread count, and architecture, then suggests the best starting preset automatically.

Current defaults use Gemma 4 variants through Ollama.

## Runtime Architecture

### Docker runtime

- frontend in Docker
- backend in Docker
- SQLite persisted in `./data`
- Teller certs mounted from `./certs`
- Ollama runs on the host for `Local AI`

Backend reaches host Ollama using:

- `http://host.docker.internal:11434`

### Local development runtime

- frontend runs locally
- backend runs locally
- `http://localhost:11434`

## Common Commands

Start:

```bash
docker compose up -d
```

Stop:

```bash
docker compose down
```

Rebuild:

```bash
docker compose up --build -d
```

View logs:

```bash
docker compose logs -f
```

## Public Demo Deploy

The safest public demo is a dedicated `demo mode` build with synthetic data, not a scrubbed copy of your real transactions. Even if merchant names are changed, real dates, amounts, and cadence can still leak too much about your finances.

Folio now includes:

- `backend/create_demo_db.py` to generate a seeded SQLite demo database
- `DEMO_MODE=true` to disable Teller/SimpleFIN enrollment and manual sync
- `Dockerfile.demo` plus `scripts/run-demo.sh` to run frontend + backend in one container for simple hosting

### Recommended setup

Use a single public demo service with:

- synthetic seeded DB
- no bank-linking UI
- no live sync
- recategorization still enabled
- ephemeral writes, so the demo naturally resets on redeploy/restart

### Local smoke test

```bash
python3 backend/create_demo_db.py --force
DEMO_MODE=true DB_FILE=backend/Folio-demo.db Folio_API_KEY=folio-demo-key VITE_API_KEY=folio-demo-key ./scripts/run-demo.sh
```

Then open `http://localhost:3000`.

### Free hosting path

The cleanest free option is a single Docker web service on Render or a similar host:

1. Push this repo to GitHub.
2. Create a new web service from the repo.
3. Point it at `Dockerfile.demo`.
4. Set these env vars:
   `DEMO_MODE=true`
   `Folio_API_KEY=folio-demo-key`
   `BACKEND_PORT=8000`
   `TRUSTED_HOSTS=<your-public-hostname>`
5. Deploy.
6. Link the public URL from your GitHub README.

Notes:

- The demo DB is regenerated on container boot, so every deploy starts clean.
- On free tiers with ephemeral filesystems, in-session edits can work and later reset. That is a good fit for a public demo.
- Do not deploy your real SQLite file to a public host.

## Configuration

Use [`.env.example`](/Users/karthikbhuvanagiri/Downloads/Folio/.env.example) as the reference template, or let `setup.py` create `.env` for you.

Important variables:

- `LLM_PROVIDER`
- `ENABLE_LOCAL_ENRICHMENT`
- `ENABLE_LLM_CATEGORIZATION`
- `ENABLE_TROVE`
- `OLLAMA_BASE_URL`
- `OLLAMA_MODEL_CATEGORIZE`
- `OLLAMA_MODEL_COPILOT`
- `ANTHROPIC_API_KEY`
- `TROVE_API_KEY`

## Project Structure

```text
Folio/
├── backend/
│   ├── main.py
│   ├── bank.py
│   ├── sanitizer.py
│   ├── enricher.py
│   ├── categorizer.py
│   ├── llm_client.py
│   ├── recurring.py
│   ├── copilot.py
│   ├── data_manager.py
│   ├── database.py
│   └── ...
├── frontend/
├── certs/
├── data/
├── docker-compose.yml
├── model_presets.json
├── setup_helpers.py
├── setup.py
└── .env.example
```

## Privacy

- Transaction data stays on your machine by default
- `Local AI` keeps enrichment and categorization on-device
- `Cloud AI` and optional Trove only send data when explicitly enabled
- Teller Connect-linked tokens are encrypted before storage
- Folio does not store your bank username or password from the Teller Connect flow
- Backend endpoints are protected by an API key

## License

MIT
