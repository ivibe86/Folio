# 🏦 Folio — Self-Hosted Personal Finance Tracker

A privacy-first personal finance app that connects to your real bank accounts,
intelligently categorizes transactions using AI, and gives you complete control
over your financial data.

**All your data stays on your machine.** No cloud. No subscriptions. No data selling.

---

## Features

- 🏦 **Real bank data** via Teller API (supports most US banks)
- 🤖 **AI categorization** — rules engine + Claude LLM for accuracy
- 🏪 **Merchant enrichment** — identifies merchants from raw bank descriptions
- 📊 **Analytics dashboard** — spending trends, category breakdowns, net worth
- 🔄 **Recurring detection** — automatically finds subscriptions
- 💬 **AI copilot** — ask questions about your finances in plain English
- 👨‍👩‍👧 **Multi-profile** — track household finances (you + spouse)
- 🔒 **Privacy-first** — PII is masked before sending to any external API

---

## Prerequisites

You'll need:
- A [Teller](https://teller.io) account with mTLS certificates
- At least one Teller access token (linked bank account)
- **Docker Desktop** (recommended) — OR Python 3.11+ and Node.js 18+

Optional:
- [Anthropic API key](https://console.anthropic.com/) — for AI features
- [Trove API key](https://trove.headline.com/) — for merchant enrichment

---

## Quick Start

### Option A: Docker (Recommended)

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/Folio.git
cd Folio

# 2. Run the interactive setup
python3 setup.py

# That's it! The setup will:
# - Guide you through configuration
# - Create your .env file
# - Build and start Docker containers
# - App runs at http://localhost:3000
```

### Option B: Local Development

# 1. Clone and setup
git clone https://github.com/yourusername/Folio.git
cd Folio
python3 setup.py  # Choose "local" when prompted

# 2. Start backend (Terminal 1)
cd backend
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uvicorn main:app --reload --port 8000

# 3. Start frontend (Terminal 2)
cd frontend
npm run dev

# 4. Open http://localhost:5173


### Configuration

All configuration is in .env (created by setup.py or copy from .env.example):


Variable	        Required	Description
TELLER_CERT_PATH	Yes	        Path to Teller mTLS certificate
TELLER_KEY_PATH 	Yes	        Path to Teller mTLS private key
*_TOKEN	            Yes         Teller access tokens (format: NAME_BANK_TOKEN=value)-- ≥1
ANTHROPIC_API_KEY	No	        Enables AI categorization and copilot
TROVE_API_KEY	    No	        Enables merchant enrichment
Folio_API_KEY 	No	        API auth key (auto-generated if blank)


### Architecture


┌────────────┐      ┌─────────────────────────────────────┐
│  Browser   │      │  Docker Compose                     │
│            │      │                                     │
│  SvelteKit │─────▶│  Frontend (Node :3000)              │
│            │      │    │                                │
│            │      │    ▼                                │
│            │      │  Backend (FastAPI :8000)             │
│            │      │    ├── Teller API (bank data)       │
│            │      │    ├── Trove API (merchant enrich)  │
│            │      │    ├── Claude API (AI categorize)   │
│            │      │    └── SQLite (./data/Folio.db)   │
└────────────┘      └─────────────────────────────────────┘


### Common Commands

# Start
docker compose up -d

# Stop
docker compose down

# View logs
docker compose logs -f

# Rebuild after code changes
docker compose up --build -d

# Sync bank data
curl -X POST http://localhost:8000/api/sync \
  -H "X-API-Key: YOUR_KEY"


### Project Structure


Folio/
├── backend/
│   ├── main.py              # FastAPI app + routes
│   ├── bank.py              # Teller API client
│   ├── sanitizer.py         # Transaction sanitization
│   ├── enricher.py          # Trove merchant enrichment
│   ├── categorizer.py       # Rule engine + LLM categorization
│   ├── recurring.py         # Subscription detection
│   ├── copilot.py           # NLP-to-SQL copilot
│   ├── data_manager.py      # Data layer + Teller sync
│   ├── database.py          # SQLite schema + migrations
│   ├── auth.py              # API key auth + rate limiting
│   ├── privacy.py           # PII masking utilities
│   └── log_config.py        # Logging configuration
├── frontend/
│   ├── src/
│   │   ├── routes/          # SvelteKit pages
│   │   └── lib/             # Components, stores, utilities
│   ├── svelte.config.js
│   └── vite.config.js
├── data/                    # SQLite database (persisted)
├── certs/                   # Teller mTLS certificates
├── docker-compose.yml
├── setup.py                 # Interactive setup script
└── .env.example             # Configuration template


### Privacy & Security

PII masking: Dollar amounts and personal names are replaced with placeholders before sending to Anthropic or Trove
Teller amount hiding: Real transaction amounts are never sent to Trove (a dummy value is used — Trove matches on description, not amount)
API key auth: All backend endpoints require authentication
Rate limiting: Per-IP limits on all endpoints
SQL safety: Copilot SQL is validated against allowlists before execution
No telemetry: Zero data leaves your machine except explicit API calls to Teller, Anthropic, and Trove


## License
MIT