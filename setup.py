#!/usr/bin/env python3
"""
Interactive setup script for Folio.

Primary supported onboarding paths:
- Docker Desktop + Local AI (host Ollama on macOS/Windows)
- Docker Desktop + Cloud AI (BYOK)
- Docker Desktop + No AI

Local development remains available for contributors who want to run the
backend/frontend outside Docker.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import json
from pathlib import Path

from setup_helpers import (
    detect_system_profile,
    format_system_profile,
    load_model_presets,
    recommend_model_preset,
)
from setup_ui import ui

ROOT_DIR = Path(__file__).parent
ENV_FILE = ROOT_DIR / ".env"
CERTS_DIR = ROOT_DIR / "certs"
DATA_DIR = ROOT_DIR / "data"
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIR = ROOT_DIR / "frontend"

OLLAMA_DOWNLOAD_URLS = {
    "macos": "https://ollama.com/download/mac",
    "windows": "https://ollama.com/download/windows",
}

DOCKER_DOWNLOAD_URLS = {
    "macos": "https://www.docker.com/products/docker-desktop/",
    "windows": "https://www.docker.com/products/docker-desktop/",
}

NODE_DOWNLOAD_URLS = {
    "macos": "https://nodejs.org/en/download",
    "windows": "https://nodejs.org/en/download",
}
MODEL_PRESETS = load_model_presets(ROOT_DIR)


def ask(prompt: str, default: str | None = None, required: bool = False) -> str:
    display = f"{prompt} [{default}]: " if default else f"{prompt}: "
    while True:
        value = input(display).strip()
        if not value and default is not None:
            return default
        if not value and required:
            ui.warning("This field is required.")
            continue
        return value


def ask_yes_no(prompt: str, default: bool = True) -> bool:
    default_label = "Y/n" if default else "y/N"
    while True:
        value = input(f"{prompt} [{default_label}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        ui.warning("Please answer yes or no.")


def detect_os() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("win"):
        return "windows"
    return "other"


def check_docker():
    docker_available = shutil.which("docker") is not None
    compose_available = False
    daemon_available = False

    if docker_available:
        try:
            compose_result = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            compose_available = compose_result.returncode == 0
        except Exception:
            pass

        try:
            daemon_result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            daemon_available = daemon_result.returncode == 0
        except Exception:
            pass

    return docker_available, compose_available, daemon_available


def check_python() -> bool:
    return sys.version_info >= (3, 11)


def check_node() -> bool:
    return shutil.which("node") is not None


def maybe_install_node(host_os: str) -> bool:
    if check_node():
        return True

    print()
    ui.warning("Node.js is not installed.")
    installed = False

    if host_os == "macos" and shutil.which("brew"):
        installed = run_install_command(["brew", "install", "node"], "Node.js")
    elif host_os == "windows" and shutil.which("winget"):
        installed = run_install_command(
            ["winget", "install", "-e", "--id", "OpenJS.NodeJS.LTS"],
            "Node.js",
        )

    if installed:
        print()
        ui.warning(
            "Node.js was installed. You may need to reopen your terminal so the node command is available."
        )
        return check_node()

    url = NODE_DOWNLOAD_URLS.get(host_os, "https://nodejs.org/en/download")
    ui.warning(f"Install Node.js 18+ from: {url}")
    if host_os == "macos":
        ui.muted("If you already use Homebrew, you can also run: brew install node")
    elif host_os == "windows":
        ui.muted("If winget is available, you can also run: winget install OpenJS.NodeJS.LTS")
    ui.muted("Then reopen your terminal and rerun setup.py.")
    return False


def setup_directories():
    DATA_DIR.mkdir(exist_ok=True)
    CERTS_DIR.mkdir(exist_ok=True)
    ui.success("Verified data/ and certs/ directories")


def setup_runtime_choice(has_docker: bool, has_local: bool) -> str:
    if has_docker and has_local:
        ui.panel(
            "Runtime",
            [
                "1. Docker (recommended)",
                "2. Local development",
            ],
            ui.BLUE,
        )
        choice = ask("  Runtime", default="1")
        return "docker" if choice in ("1", "docker", "d", "") else "local"
    if has_docker:
        return "docker"
    return "local"


def gather_teller_config():
    ui.panel(
        "Teller Setup",
        [
            "Folio uses Teller for all bank connectivity.",
            "Have these ready before continuing:",
            "• your Teller mTLS certificate",
            "• your Teller private key",
            "• your Teller App ID for the recommended Teller Connect flow",
            "• a plan to link in the UI, or manual access tokens",
        ],
        ui.BLUE,
    )
    ui.panel(
        "Quick Checklist",
        [
            "Sign up at https://teller.io",
            "Create an application and copy its App ID",
            "Download your mTLS certificate and private key",
            "Place them in ./certs as teller-cert.pem and teller-key.pem",
            "If you prefer manual setup, create access tokens in the Teller dashboard",
        ],
        ui.CYAN,
    )

    ready = ask_yes_no("  Do you have your Teller credentials ready and want to continue?", default=True)
    if not ready:
        print()
        ui.warning("Please gather your Teller credentials first, then rerun setup.py.")
        sys.exit(1)

    print()
    ui.panel(
        "Certificate Files",
        [
            "Folio expects these files in ./certs before first sync:",
            "certs/teller-cert.pem",
            "certs/teller-key.pem",
        ],
        ui.BLUE,
    )

    cert_path = "certs/teller-cert.pem"
    key_path = "certs/teller-key.pem"

    cert_full = ROOT_DIR / cert_path
    key_full = ROOT_DIR / key_path
    if not cert_full.exists():
        ui.warning(f"Certificate not found at {cert_path}")
    if not key_full.exists():
        ui.warning(f"Key not found at {key_path}")

    print()
    ui.panel(
        "Account Linking",
        [
            "1. Recommended: connect accounts through the UI with Teller Connect",
            "   - easier setup",
            "   - Folio does not store your bank username or password",
            "   - linked Teller tokens are encrypted before storage in the database",
            "2. Advanced: add Teller access tokens manually in setup/.env",
            "   - useful if you prefer managing tokens yourself",
        ],
        ui.CYAN,
    )
    teller_mode_choice = ask("  Teller setup mode", default="1").lower()
    teller_mode = "connect" if teller_mode_choice in ("1", "connect", "ui", "") else "manual"

    if teller_mode == "connect":
        print()
        ui.success("Teller Connect is the recommended setup path.")
        teller_app_id = ask("  Teller App ID", required=True)
    else:
        print()
        ui.warning("Manual token mode selected.")
        ui.panel(
            "Manual Token Guide",
            [
                "Create or manage access tokens from the Teller dashboard",
                "Add them now, or later in .env as FIRSTNAME_BANKNAME_TOKEN=value",
                "Example: JOHN_BOFA_TOKEN=test_tok_abc123",
                "You can still add a Teller App ID later for UI-based linking",
            ],
            ui.YELLOW,
        )
        ui.muted("You can skip Teller App ID now if you plan to stay on manual tokens.")
        ui.muted("If you want the UI-based Teller Connect flow later, add the App ID and rebuild the frontend.")
        teller_app_id = ask("  Teller App ID (optional for manual mode)")

    teller_env = "development"

    print()
    ui.info("Tokens enrolled through the UI are encrypted before storage.")
    try:
        from cryptography.fernet import Fernet

        encryption_key = Fernet.generate_key().decode()
    except ImportError:
        import base64
        import secrets

        encryption_key = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()

    ui.success("Generated token encryption key and will store it in .env")

    tokens: dict[str, str] = {}
    if teller_mode == "manual":
        print()
        ui.info("Add Teller access tokens manually now if you want.")
        while True:
            token_name = ask(
                "  Token variable name (e.g. JOHN_BOFA_TOKEN, press Enter to finish)",
            )
            if not token_name:
                break
            token_value = ask(f"  Value for {token_name}", required=True)
            tokens[token_name] = token_value

    return cert_path, key_path, tokens, teller_app_id, teller_env, encryption_key


def choose_ai_mode(system_profile: dict) -> str:
    recommended_preset = MODEL_PRESETS[recommend_model_preset(system_profile, MODEL_PRESETS)]
    ui.panel(
        "AI Modes",
        [
            f"1. Local AI (recommended for your system: {recommended_preset['label']})",
            "2. Cloud AI (Bring your own key)",
            "3. No AI",
        ],
        ui.BLUE,
    )
    choice = ask("  AI mode", default="1").lower()
    if choice in ("1", "local", "local ai", ""):
        return "local"
    if choice in ("2", "cloud", "cloud ai"):
        return "cloud"
    return "none"


def choose_model_preset(system_profile: dict) -> dict:
    recommended_key = recommend_model_preset(system_profile, MODEL_PRESETS)
    recommended_preset = MODEL_PRESETS[recommended_key]

    ui.info("Detected system profile:")
    ui.kv("System", format_system_profile(system_profile))
    ui.kv(
        "Recommendation",
        f"{recommended_preset['label']} (~{recommended_preset['disk_gb']} GB download)",
    )
    if system_profile.get("ram_gb") is not None and system_profile["ram_gb"] < 16:
        ui.muted("Note: lower-memory systems are better off starting with Light.")

    ui.info("Choose a local model preset:")
    for idx, (key, preset) in enumerate(MODEL_PRESETS.items(), start=1):
        suffix = " (recommended for your system)" if key == recommended_key else ""
        ui.muted(
            f"    {idx}. {preset['label']} — {preset['description']} "
            f"(~{preset['disk_gb']} GB){suffix}"
        )
    default_choice = {
        "light": "1",
        "balanced": "2",
        "quality": "3",
    }[recommended_key]
    choice = ask("  Model preset", default=default_choice)
    selected_key = {
        "1": "light",
        "2": "balanced",
        "3": "quality",
        "light": "light",
        "balanced": "balanced",
        "quality": "quality",
    }.get(choice.lower(), "balanced")
    return MODEL_PRESETS[selected_key]


def check_ollama_cli() -> bool:
    return shutil.which("ollama") is not None


def check_ollama_server(base_url: str = "http://localhost:11434", timeout: float = 2.0) -> bool:
    url = f"{base_url.rstrip('/')}/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False


def get_ollama_models(base_url: str = "http://localhost:11434", timeout: float = 3.0) -> set[str]:
    url = f"{base_url.rstrip('/')}/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return set()

    models = set()
    for item in payload.get("models", []):
        name = item.get("name")
        if isinstance(name, str) and name:
            models.add(name)
    return models


def run_install_command(command: list[str], label: str) -> bool:
    print()
    ui.info(f"Installing {label}...")
    try:
        subprocess.run(command, check=True)
        return True
    except subprocess.CalledProcessError as exc:
        ui.error(f"Failed to install {label}: {exc}")
        return False


def maybe_install_ollama(host_os: str) -> bool:
    if check_ollama_cli():
        return True

    print()
    ui.warning("Ollama is not installed yet.")
    installed = False

    if host_os == "macos" and shutil.which("brew"):
        installed = run_install_command(["brew", "install", "--cask", "ollama"], "Ollama")
    elif host_os == "windows" and shutil.which("winget"):
        installed = run_install_command(
            ["winget", "install", "-e", "--id", "Ollama.Ollama"],
            "Ollama",
        )

    if installed:
        print()
        ui.warning(
            "Ollama was installed. You may need to restart your shell, launch the Ollama app, and in some cases restart the system before setup can finish cleanly."
        )
        return check_ollama_cli()

    url = OLLAMA_DOWNLOAD_URLS.get(host_os, "https://ollama.com/download")
    ui.warning(f"Please install Ollama from: {url}")
    ui.muted("Then rerun setup.py.")
    return False


def ensure_ollama_running(host_os: str) -> bool:
    if check_ollama_server():
        return True

    print()
    ui.warning("Ollama is installed but its local API is not responding on http://localhost:11434.")
    if host_os == "macos":
        ui.muted("Launch the Ollama app once so it can start its background server and link the CLI.")
    elif host_os == "windows":
        ui.muted("Launch Ollama once from the Start Menu so the background server can start.")

    if check_ollama_cli():
        ui.info("Trying to start Ollama server automatically...")
        kwargs = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if sys.platform.startswith("win"):
            kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        else:
            kwargs["start_new_session"] = True
        try:
            subprocess.Popen(["ollama", "serve"], **kwargs)
            for _ in range(15):
                if check_ollama_server():
                    return True
                time.sleep(1)
        except Exception:
            pass

    ui.error("Ollama still is not reachable. Start/restart Ollama manually and rerun setup if needed.")
    return False


def pull_ollama_model(model: str):
    ui.info(f"Pulling model {model}...")
    subprocess.run(["ollama", "pull", model], check=True)


def ensure_ollama_model(model: str, available_models: set[str]) -> set[str]:
    if model in available_models:
        ui.success(f"Found existing model: {model} (skipping download)")
        return available_models

    pull_ollama_model(model)
    refreshed_models = get_ollama_models()
    if model in refreshed_models:
        return refreshed_models
    available_models.add(model)
    return available_models


def gather_ai_config(ai_mode: str, runtime_mode: str, host_os: str) -> dict:
    internal_trove_seed = hashlib.sha256(str(ROOT_DIR).encode("utf-8")).hexdigest()[:24]
    config = {
        "ai_mode": ai_mode,
        "llm_provider": "anthropic",
        "anthropic_key": "",
        "trove_key": "",
        "trove_seed": internal_trove_seed,
        "enable_trove": "false",
        "enable_local_enrichment": "false",
        "enable_llm_categorization": "false",
        "ollama_base_url": "",
        "ollama_model_categorize": "",
        "ollama_model_copilot": "",
        "local_enrichment_batch_size": "20",
        "local_enrichment_min_confidence": "medium",
    }

    if ai_mode == "local":
        if host_os not in {"macos", "windows"}:
            print("  Local AI installer flow currently targets macOS and Windows.")
            print("  Falling back to No AI mode on this platform.")
            return config

        if not maybe_install_ollama(host_os):
            print("  Local AI setup could not continue without Ollama.")
            sys.exit(1)

        if not ensure_ollama_running(host_os):
            print("  Ollama is required for Local AI mode.")
            sys.exit(1)

        system_profile = detect_system_profile(host_os)
        preset = choose_model_preset(system_profile)
        print(
            f"\n  Local AI preset: {preset['label']}"
            f"\n    Categorization/enrichment: {preset['categorize_model']}"
            f"\n    Copilot: {preset['copilot_model']}"
            f"\n    Approx disk for models: {preset['disk_gb']} GB"
        )

        available_models = get_ollama_models()
        available_models = ensure_ollama_model(preset["categorize_model"], available_models)
        if preset["copilot_model"] != preset["categorize_model"]:
            available_models = ensure_ollama_model(preset["copilot_model"], available_models)

        config.update(
            {
                "llm_provider": "ollama",
                "enable_local_enrichment": "true",
                "enable_llm_categorization": "true",
                "enable_trove": "false",
                "ollama_base_url": (
                    "http://host.docker.internal:11434"
                    if runtime_mode == "docker"
                    else "http://localhost:11434"
                ),
                "ollama_model_categorize": preset["categorize_model"],
                "ollama_model_copilot": preset["copilot_model"],
            }
        )
        return config

    if ai_mode == "cloud":
        print()
        ui.info("Cloud AI mode uses your API key and keeps Docker packaging unchanged.")
        anthropic_key = ask("  Anthropic API key", required=True)
        trove_key = ask("  Trove API key (press Enter to skip)")
        config.update(
            {
                "llm_provider": "anthropic",
                "anthropic_key": anthropic_key,
                "trove_key": trove_key,
                "enable_trove": "true" if trove_key else "false",
                "enable_local_enrichment": "false",
                "enable_llm_categorization": "true",
            }
        )
        return config

    print()
    ui.info("No AI mode keeps only deterministic rules and manual categorization.")
    return config


def gather_security_config():
    ui.info("An API key protects your backend from unauthorized access.")
    ui.muted("Folio will generate one automatically and write it to .env.")
    print()
    import secrets

    api_key = secrets.token_urlsafe(32)
    ui.success("Generated backend API key automatically.")
    return api_key


def write_env_file(config: dict):
    ai_mode = config["ai_mode"]
    lines = [
        "# ==============================================================",
        "# Folio Configuration",
        "# Generated by setup.py",
        "# ==============================================================",
        "",
        f"# AI mode selected during setup: {ai_mode}",
        "",
        "# -- Teller Certificates --",
        f"TELLER_CERT_PATH={config['cert_path']}",
        f"TELLER_KEY_PATH={config['key_path']}",
        "",
        "# -- Teller Connect --",
        f"TELLER_APPLICATION_ID={config.get('teller_app_id', '')}",
        f"TELLER_ENVIRONMENT={config.get('teller_env', 'sandbox')}",
        "",
        "# -- Token Encryption --",
        f"TOKEN_ENCRYPTION_KEY={config.get('encryption_key', '')}",
        "",
        "# -- Teller Access Tokens (legacy / manual) --",
    ]

    for name, value in config.get("tokens", {}).items():
        lines.append(f"{name}={value}")
    if not config.get("tokens"):
        lines.extend([
            "# Add tokens here: FIRSTNAME_BANKNAME_TOKEN=value",
            "# Or link accounts from the UI after setup",
        ])

    lines.extend(
        [
            "",
            "# -- Security --",
            f"Folio_API_KEY={config.get('api_key', '')}",
            "",
            "# -- Frontend --",
            f"VITE_API_KEY={config.get('api_key', '')}",
            f"VITE_TELLER_APP_ID={config.get('teller_app_id', '')}",
            f"VITE_TELLER_ENVIRONMENT={config.get('teller_env', 'sandbox')}",
            "",
            "# -- Feature Toggles --",
            f"ENABLE_TROVE={config.get('enable_trove', 'false')}",
            f"ENABLE_LOCAL_ENRICHMENT={config.get('enable_local_enrichment', 'false')}",
            f"ENABLE_LLM_CATEGORIZATION={config.get('enable_llm_categorization', 'false')}",
            "COPILOT_MAX_WRITE_ROWS=5000",
            "",
            "# -- LLM Provider --",
            f"LLM_PROVIDER={config.get('llm_provider', 'anthropic')}",
            "",
            "# -- Ollama (Local AI mode) --",
            f"OLLAMA_BASE_URL={config.get('ollama_base_url', '')}",
            f"OLLAMA_MODEL_CATEGORIZE={config.get('ollama_model_categorize', '')}",
            f"OLLAMA_MODEL_COPILOT={config.get('ollama_model_copilot', '')}",
            f"LOCAL_ENRICHMENT_BATCH_SIZE={config.get('local_enrichment_batch_size', '20')}",
            f"LOCAL_ENRICHMENT_MIN_CONFIDENCE={config.get('local_enrichment_min_confidence', 'medium')}",
            "OLLAMA_TIMEOUT_CATEGORIZE=600",
            "OLLAMA_TIMEOUT_COPILOT=240",
            "",
            "# -- Cloud AI (BYOK) --",
            f"ANTHROPIC_API_KEY={config.get('anthropic_key', '')}",
            "",
            "# -- Trove Merchant Enrichment --",
            f"TROVE_API_KEY={config.get('trove_key', '')}",
            f"TROVE_USER_SEED={config.get('trove_seed', 'Folio-self-hosted')}",
            "",
            "# -- App Settings --",
            "CORS_ORIGINS=http://localhost:5173,http://localhost:3000",
            "",
            "# -- Docker Settings --",
            "BACKEND_PORT=8000",
            "FRONTEND_PORT=3000",
            "DB_FILE=Folio.db",
            "",
        ]
    )

    ENV_FILE.write_text("\n".join(lines), encoding="utf-8")
    ui.success("Configuration written to .env")


def copy_env_for_local():
    backend_env = BACKEND_DIR / ".env"
    frontend_env = FRONTEND_DIR / ".env"
    shutil.copy(str(ENV_FILE), str(backend_env))
    shutil.copy(str(ENV_FILE), str(frontend_env))
    ui.success("Copied .env to backend/ and frontend/")


def start_docker():
    print()
    ui.info("Building and starting containers...")
    ui.muted("This may take a few minutes on first run.")
    print()
    try:
        subprocess.run(
            ["docker", "compose", "up", "--build", "-d"],
            cwd=str(ROOT_DIR),
            check=True,
        )
        print()
        ui.success("Folio is running.")
        ui.panel(
            "Services",
            [
                "Frontend:  http://localhost:3000",
                "Backend:   http://localhost:8000",
                "",
                "Stop:      docker compose down",
                "Logs:      docker compose logs -f",
                "Restart:   docker compose restart",
            ],
            ui.CYAN,
        )
    except subprocess.CalledProcessError as exc:
        print()
        ui.error(f"Docker startup failed: {exc}")
        ui.muted("If Docker Desktop was just installed, start/restart it and rerun setup.")


def start_local():
    print()
    ui.info("Preparing local development setup...")
    print()

    if not check_node():
        if not maybe_install_node(detect_os()):
            sys.exit(1)

    copy_env_for_local()

    venv_dir = BACKEND_DIR / ".venv"
    if not venv_dir.exists():
        ui.info("Creating Python virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)

    if sys.platform.startswith("win"):
        pip = str(venv_dir / "Scripts" / "pip")
        python = str(venv_dir / "Scripts" / "python")
    else:
        pip = str(venv_dir / "bin" / "pip")
        python = str(venv_dir / "bin" / "python")

    ui.info("Installing backend dependencies...")
    subprocess.run([pip, "install", "-r", str(BACKEND_DIR / "requirements.txt")], check=True)

    if not (FRONTEND_DIR / "node_modules").exists():
        ui.info("Installing frontend dependencies...")
        lockfile = FRONTEND_DIR / "package-lock.json"
        npm_cmd = ["npm", "ci"] if lockfile.exists() else ["npm", "install"]
        if not lockfile.exists():
            ui.muted("No package-lock.json found, so using npm install instead of npm ci.")
        subprocess.run(npm_cmd, cwd=str(FRONTEND_DIR), check=True)

    print()
    ui.success("Setup complete.")
    ui.panel(
        "Run Locally",
        [
            f"Backend:  cd backend && {python} -m uvicorn main:app --port 8000",
            "Frontend: cd frontend && npm run dev",
            "Open:     http://localhost:5173",
        ],
        ui.CYAN,
    )


def main():
    ui.banner()

    host_os = detect_os()
    system_profile = detect_system_profile(host_os)
    docker_ok, compose_ok, docker_daemon_ok = check_docker()
    python_ok = check_python()
    node_ok = check_node()

    ui.info("Checking prerequisites...")
    print()
    ui.kv("Operating system", host_os)
    ui.kv("Docker CLI", "installed" if docker_ok else "not found")
    ui.kv("Docker Compose", "installed" if compose_ok else "not found")
    ui.kv("Docker daemon", "running" if docker_daemon_ok else "not ready")
    ui.kv("Python 3.11+", "yes" if python_ok else "no")
    ui.kv("Node.js", "installed" if node_ok else "not found")

    has_docker = docker_ok and compose_ok
    has_local = python_ok and node_ok

    if not has_docker and not has_local:
        print()
        ui.error("Neither Docker nor Python+Node were detected.")
        if host_os in DOCKER_DOWNLOAD_URLS:
            ui.muted(f"Install Docker Desktop: {DOCKER_DOWNLOAD_URLS[host_os]}")
            ui.muted("Then reopen your terminal and rerun setup.py.")
        if host_os in NODE_DOWNLOAD_URLS:
            ui.muted(f"Install Node.js 18+: {NODE_DOWNLOAD_URLS[host_os]}")
        ui.muted("For local development, install Python 3.11+ and Node.js 18+.")
        sys.exit(1)

    if ENV_FILE.exists():
        overwrite = ask("\n  .env already exists. Overwrite it?", default="no").lower()
        if overwrite not in ("y", "yes"):
            ui.info("Keeping existing .env.")
            runtime_mode = setup_runtime_choice(has_docker, has_local)
            if runtime_mode == "docker":
                if not docker_daemon_ok:
                    ui.warning("Docker Desktop is installed but not ready. Start/restart Docker Desktop first.")
                    sys.exit(1)
                start_docker()
            else:
                start_local()
            return

    if has_docker and not docker_daemon_ok:
        print()
        ui.warning(
            "Docker Desktop looks installed but its daemon is not ready. If you just installed Docker Desktop, start it and restart your shell or system if needed."
        )

    ui.step(1, "Directory Setup")
    setup_directories()

    runtime_mode = setup_runtime_choice(has_docker, has_local)

    ui.step(2, "Teller Bank Connection")
    cert_path, key_path, tokens, teller_app_id, teller_env, encryption_key = gather_teller_config()

    ui.step(3, "AI Mode")
    ai_mode = choose_ai_mode(system_profile)
    ai_config = gather_ai_config(ai_mode, runtime_mode, host_os)

    ui.step(4, "Security")
    api_key = gather_security_config()

    ui.step(5, "Writing Configuration")
    write_env_file(
        {
            "cert_path": cert_path,
            "key_path": key_path,
            "tokens": tokens,
            "teller_app_id": teller_app_id,
            "teller_env": teller_env,
            "encryption_key": encryption_key,
            "api_key": api_key,
            **ai_config,
        }
    )

    ui.step(6, "Start Application")
    if runtime_mode == "docker":
        if not docker_daemon_ok:
            ui.warning("Docker Desktop is not ready yet.")
            ui.panel(
                "Next Step",
                ["Start/restart Docker Desktop, then run:", "docker compose up --build -d"],
                ui.YELLOW,
            )
            return
        start_docker()
    else:
        start_local()


if __name__ == "__main__":
    main()
