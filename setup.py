#!/usr/bin/env python3
"""
setup.py
Interactive setup script for Folio.
Guides users through configuration and starts the app.
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

ROOT_DIR = Path(__file__).parent
ENV_FILE = ROOT_DIR / ".env"
ENV_EXAMPLE = ROOT_DIR / "backend" / ".env_example.txt"
CERTS_DIR = ROOT_DIR / "certs"
DATA_DIR = ROOT_DIR / "data"
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIR = ROOT_DIR / "frontend"


def print_banner():
    print("\n" + "=" * 60)
    print("  🏦  Folio — Personal Finance Tracker Setup")
    print("=" * 60 + "\n")


def print_step(num, title):
    print(f"\n{'─' * 50}")
    print(f"  Step {num}: {title}")
    print(f"{'─' * 50}\n")


def ask(prompt, default=None, required=False, secret=False):
    """Prompt user for input with optional default."""
    if default:
        display = f"{prompt} [{default}]: "
    else:
        display = f"{prompt}: "

    while True:
        value = input(display).strip()
        if not value and default:
            return default
        if not value and required:
            print("  ⚠  This field is required.")
            continue
        if not value and not required:
            return ""
        return value


def check_docker():
    """Check if Docker and Docker Compose are available."""
    docker_available = shutil.which("docker") is not None
    compose_available = False

    if docker_available:
        try:
            result = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True, text=True, timeout=10,
            )
            compose_available = result.returncode == 0
        except Exception:
            pass

    return docker_available, compose_available


def check_python():
    """Check Python version."""
    version = sys.version_info
    return version >= (3, 11)


def check_node():
    """Check if Node.js is available."""
    return shutil.which("node") is not None


def setup_directories():
    """Create required directories."""
    DATA_DIR.mkdir(exist_ok=True)
    CERTS_DIR.mkdir(exist_ok=True)
    print("  ✓ Created data/ and certs/ directories")


def gather_teller_config():
    """Gather Teller API configuration."""
    print("  Teller provides bank connectivity via mTLS certificates.")
    print("  You need a certificate pair (.pem files) from https://teller.io")
    print("  Place them in the certs/ directory.\n")

    cert_path = ask(
        "  Path to Teller certificate (relative to project root)",
        default="certs/teller-cert.pem",
    )
    key_path = ask(
        "  Path to Teller private key (relative to project root)",
        default="certs/teller-key.pem",
    )

    # Check if files exist
    cert_full = ROOT_DIR / cert_path
    key_full = ROOT_DIR / key_path
    if not cert_full.exists():
        print(f"  ⚠  Certificate not found at {cert_path}")
        print(f"     Place your certificate there before starting the app.")
    if not key_full.exists():
        print(f"  ⚠  Key not found at {key_path}")
        print(f"     Place your key there before starting the app.")

    # Teller Connect config
    print("\n  Teller Connect lets users link bank accounts from the UI.")
    print("  You can find your App ID in the Teller developer dashboard.\n")

    teller_app_id = ask(
        "  Teller App ID (press Enter to skip — can add later)",
    )
    teller_env = ask(
        "  Teller environment (sandbox/development/production)",
        default="sandbox",
    )

    # Token encryption key
    print("\n  Tokens enrolled via the UI are encrypted before storage.")
    print("  An encryption key will be auto-generated for you.\n")

    try:
        from cryptography.fernet import Fernet
        encryption_key = Fernet.generate_key().decode()
    except ImportError:
        import secrets as _sec
        import base64
        encryption_key = base64.urlsafe_b64encode(_sec.token_bytes(32)).decode()

    print(f"  ✓ Generated encryption key (stored in .env)")
    print(f"  ⚠  Back up your .env file — if you lose this key,")
    print(f"     enrolled tokens cannot be decrypted.")

    # Legacy manual tokens
    print("\n  You can also add bank tokens manually (legacy method).")
    print("  Format: FIRSTNAME_BANKNAME_TOKEN=token_value")
    print("  Or press Enter to skip — you'll link accounts from the UI instead.\n")

    tokens = {}
    while True:
        token_name = ask(
            "  Token variable name (e.g., JOHN_BOFA_TOKEN, or press Enter to finish)",
        )
        if not token_name:
            break
        token_value = ask(f"  Value for {token_name}", required=True)
        tokens[token_name] = token_value

    if not tokens and not teller_app_id:
        print("  ⚠  No tokens or Teller App ID configured.")
        print("     Add at least one to .env before using the app.")

    return cert_path, key_path, tokens, teller_app_id, teller_env, encryption_key


def gather_api_keys():
    """Gather optional API keys."""
    print("  These are optional but enhance the experience:\n")

    anthropic_key = ask(
        "  Anthropic API key (for AI categorization + copilot, press Enter to skip)",
    )
    trove_key = ask(
        "  Trove API key (for merchant enrichment, press Enter to skip)",
    )
    trove_seed = ask(
        "  Trove user seed (any random string for anonymization)",
        default="Folio-self-hosted",
    )

    skipped = []
    if not anthropic_key:
        skipped.append("Anthropic (AI categorization will be disabled)")
    if not trove_key:
        skipped.append("Trove (merchant enrichment will be disabled)")

    if skipped:
        print(f"\n  ℹ  Skipped: {', '.join(skipped)}")
        print("     You can add these to .env later.")

    return anthropic_key, trove_key, trove_seed


def gather_security_config():
    """Gather security configuration."""
    print("  An API key protects your backend from unauthorized access.\n")

    import secrets
    default_key = secrets.token_urlsafe(32)

    api_key = ask(
        "  Folio API key (press Enter to auto-generate)",
        default=default_key,
    )

    return api_key


def write_env_file(config: dict):
    """Write the .env file from gathered configuration."""
    lines = [
        "# ==============================================================",
        "# Folio Configuration",
        "# Generated by setup.py",
        "# ==============================================================",
        "",
        "# -- Teller Certificates --",
        f"TELLER_CERT_PATH={config['cert_path']}",
        f"TELLER_KEY_PATH={config['key_path']}",
        "",
        "# -- Teller Connect --",
        f"TELLER_APPLICATION_ID={config.get('teller_app_id', '')}",
        f"TELLER_ENVIRONMENT={config.get('teller_env', 'sandbox')}",
        "",
        "# -- Token Encryption (do NOT lose this -- back up your .env) --",
        f"TOKEN_ENCRYPTION_KEY={config.get('encryption_key', '')}",
        "",
        "# -- Teller Access Tokens (legacy / manual) --",
    ]

    for name, value in config.get("tokens", {}).items():
        lines.append(f"{name}={value}")

    if not config.get("tokens"):
        lines.append("# Add tokens here: FIRSTNAME_BANKNAME_TOKEN=value")
        lines.append("# Or link accounts from the UI using the + button")

    lines.extend([
        "",
        "# -- API Keys (Optional) --",
        f"ANTHROPIC_API_KEY={config.get('anthropic_key', '')}",
        f"TROVE_API_KEY={config.get('trove_key', '')}",
        f"TROVE_USER_SEED={config.get('trove_seed', 'Folio-self-hosted')}",
        "",
        "# -- Security --",
        f"Folio_API_KEY={config.get('api_key', '')}",
        "",
        "# -- App Settings --",
        "CORS_ORIGINS=http://localhost:5173,http://localhost:3000",
        "COPILOT_MAX_WRITE_ROWS=5000",
        "ENABLE_TROVE=true",
        "ENABLE_LLM_CATEGORIZATION=true",
        "",
        "# -- Docker Settings --",
        "BACKEND_PORT=8000",
        "FRONTEND_PORT=3000",
        f"DB_FILE=Folio.db",
        "",
        "# -- Frontend (passed to Vite at build time) --",
        f"VITE_API_KEY={config.get('api_key', '')}",
        f"VITE_TELLER_APP_ID={config.get('teller_app_id', '')}",
        f"VITE_TELLER_ENVIRONMENT={config.get('teller_env', 'sandbox')}",
    ])

    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  ✓ Configuration written to .env")


def copy_env_for_local():
    """Copy root .env to backend/ and frontend/ for local development."""
    backend_env = BACKEND_DIR / ".env"
    frontend_env = FRONTEND_DIR / ".env"

    if not backend_env.exists():
        shutil.copy(str(ENV_FILE), str(backend_env))
        print("  ✓ Copied .env to backend/")

    if not frontend_env.exists():
        shutil.copy(str(ENV_FILE), str(frontend_env))
        print("  ✓ Copied .env to frontend/")


def start_docker():
    """Build and start Docker containers."""
    print("\n  Building and starting containers...")
    print("  (This may take a few minutes on first run)\n")

    try:
        subprocess.run(
            ["docker", "compose", "up", "--build", "-d"],
            cwd=str(ROOT_DIR),
            check=True,
        )
        print("\n  ✅ Folio is running!")
        print("  ╔══════════════════════════════════════════╗")
        print("  ║  Frontend:  http://localhost:3000        ║")
        print("  ║  Backend:   http://localhost:8000        ║")
        print("  ║                                          ║")
        print("  ║  Stop:      docker compose down          ║")
        print("  ║  Logs:      docker compose logs -f       ║")
        print("  ║  Restart:   docker compose restart       ║")
        print("  ╚══════════════════════════════════════════╝")
    except subprocess.CalledProcessError as e:
        print(f"\n  ❌ Docker startup failed: {e}")
        print("     Check 'docker compose logs' for details.")


def start_local():
    """Start without Docker using local Python + Node."""
    print("\n  Starting in local mode...\n")

    # Copy .env to subdirectories for local dev
    copy_env_for_local()

    # Backend
    venv_dir = BACKEND_DIR / ".venv"
    if not venv_dir.exists():
        print("  Creating Python virtual environment...")
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            check=True,
        )

    # Determine pip path
    if sys.platform == "win32":
        pip = str(venv_dir / "Scripts" / "pip")
        python = str(venv_dir / "Scripts" / "python")
    else:
        pip = str(venv_dir / "bin" / "pip")
        python = str(venv_dir / "bin" / "python")

    print("  Installing Python dependencies...")
    subprocess.run(
        [pip, "install", "-r", str(BACKEND_DIR / "requirements.txt")],
        capture_output=True,
        check=True,
    )

    # Frontend
    if not (FRONTEND_DIR / "node_modules").exists():
        print("  Installing Node dependencies...")
        subprocess.run(
            ["npm", "ci"],
            cwd=str(FRONTEND_DIR),
            capture_output=True,
            check=True,
        )

    print("\n  ✅ Setup complete! Start the app with:\n")
    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║  Terminal 1 (backend):                                  ║")
    print(f"  ║    cd backend && {python} -m uvicorn main:app --port 8000 ║")
    print("  ║                                                         ║")
    print("  ║  Terminal 2 (frontend):                                 ║")
    print("  ║    cd frontend && npm run dev                           ║")
    print("  ║                                                         ║")
    print("  ║  Then open http://localhost:5173                        ║")
    print("  ╚══════════════════════════════════════════════════════════╝")


def main():
    print_banner()

    # Check prerequisites
    print("  Checking prerequisites...\n")
    docker_ok, compose_ok = check_docker()
    python_ok = check_python()
    node_ok = check_node()

    print(f"  Docker:         {'✓ installed' if docker_ok else '✗ not found'}")
    print(f"  Docker Compose: {'✓ installed' if compose_ok else '✗ not found'}")
    print(f"  Python 3.11+:   {'✓ ' + sys.version.split()[0] if python_ok else '✗ need 3.11+'}")
    print(f"  Node.js:        {'✓ installed' if node_ok else '✗ not found'}")

    has_docker = docker_ok and compose_ok
    has_local = python_ok and node_ok

    if not has_docker and not has_local:
        print("\n  ❌ Neither Docker nor Python+Node found.")
        print("     Install Docker Desktop: https://docker.com/products/docker-desktop")
        print("     Or install Python 3.11+ and Node.js 18+")
        sys.exit(1)

    # Check for existing .env
    if ENV_FILE.exists():
        overwrite = ask(
            "\n  .env file already exists. Overwrite?",
            default="no",
        )
        if overwrite.lower() not in ("yes", "y"):
            print("  Keeping existing .env file.")
            # Skip to start
            if has_docker:
                run_docker = ask("\n  Start with Docker?", default="yes")
                if run_docker.lower() in ("yes", "y"):
                    start_docker()
                    return
            if has_local:
                start_local()
            return

    # Setup directories
    print_step(1, "Directory Setup")
    setup_directories()

    # Teller config
    print_step(2, "Teller Bank Connection")
    cert_path, key_path, tokens, teller_app_id, teller_env, encryption_key = gather_teller_config()

    # API keys
    print_step(3, "API Keys (Optional)")
    anthropic_key, trove_key, trove_seed = gather_api_keys()

    # Security
    print_step(4, "Security")
    api_key = gather_security_config()

    # Write .env
    print_step(5, "Writing Configuration")
    write_env_file({
        "cert_path": cert_path,
        "key_path": key_path,
        "tokens": tokens,
        "teller_app_id": teller_app_id,
        "teller_env": teller_env,
        "encryption_key": encryption_key,
        "anthropic_key": anthropic_key,
        "trove_key": trove_key,
        "trove_seed": trove_seed,
        "api_key": api_key,
    })

    # Choose deployment mode
    print_step(6, "Start Application")

    if has_docker and has_local:
        choice = ask(
            "  Run with Docker (recommended) or local? [docker/local]",
            default="docker",
        )
        use_docker = choice.lower() in ("docker", "d", "")
    elif has_docker:
        use_docker = True
    else:
        use_docker = False

    if use_docker:
        start_docker()
    else:
        start_local()


if __name__ == "__main__":
    main()