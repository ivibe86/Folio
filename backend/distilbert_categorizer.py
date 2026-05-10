"""Optional local DistilBERT categorizer for broad transaction categories.

This module intentionally keeps Hugging Face / ONNX imports lazy so Folio can
run without ML dependencies unless ``CATEGORIZATION_BACKEND=distilbert`` is
selected.
"""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

DEFAULT_HF_MODEL = "DoDataThings/distilbert-us-transaction-classifier-v2"
DISTILBERT_LABELS = (
    "Restaurants",
    "Groceries",
    "Shopping",
    "Transportation",
    "Entertainment",
    "Utilities",
    "Subscription",
    "Healthcare",
    "Insurance",
    "Mortgage",
    "Rent",
    "Travel",
    "Education",
    "Personal Care",
    "Transfer",
    "Income",
    "Fees",
)

DIRECT_FOLIO_MAPPING = {
    "restaurants": "Food & Dining",
    "groceries": "Groceries",
    "shopping": "Shopping",
    "transportation": "Transportation",
    "entertainment": "Entertainment",
    "utilities": "Utilities",
    "subscription": "Subscriptions",
    "healthcare": "Healthcare",
    "insurance": "Insurance",
    "travel": "Travel",
    "fees": "Fees & Charges",
}

CONDITIONAL_FOLIO_MAPPING = {
    "rent": "Housing",
    "mortgage": "Housing",
    "education": "Education",
    "personal care": "Personal Care",
}

BLOCKED_LABELS = {"transfer", "income"}
MODEL_PROTECTED_SUGGESTIONS = {
    "Cash Deposit",
    "Cash Withdrawal",
    "Credit Card Payment",
    "Credits & Refunds",
    "Income",
    "Investment Transfer",
    "Personal Transfer",
    "Savings Transfer",
}
_TRUE_VALUES = {"1", "true", "yes", "on"}
_PREDICTOR_CACHE: dict[tuple[Any, ...], "DistilbertLoadResult"] = {}
_UNAVAILABLE_WARNED: set[tuple[str, tuple[str, ...]]] = set()


@dataclass(frozen=True)
class DistilbertConfig:
    model_name: str
    model_path: Path | None
    allow_download: bool
    local_files_only: bool
    required: bool
    threshold: float
    batch_size: int
    shadow: bool
    use_stub: bool

    @property
    def cache_key(self) -> tuple[Any, ...]:
        return (
            self.model_name,
            str(self.model_path) if self.model_path else "",
            self.allow_download,
            self.local_files_only,
            self.use_stub,
        )


@dataclass(frozen=True)
class DistilbertLoadResult:
    predictor: Any | None
    model_id: str
    warnings: tuple[str, ...]

    @property
    def available(self) -> bool:
        return self.predictor is not None


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in _TRUE_VALUES


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid %s=%s; using %.2f", name, raw, default)
        return default
    if not 0.0 <= value <= 1.0:
        logger.warning("Invalid %s=%s; using %.2f", name, raw, default)
        return default
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%s; using %d", name, raw, default)
        return default
    return max(1, value)


def get_config() -> DistilbertConfig:
    model_path = os.getenv("DISTILBERT_MODEL_PATH", "").strip()
    allow_download = _env_bool("DISTILBERT_ALLOW_DOWNLOAD", "false")
    return DistilbertConfig(
        model_name=os.getenv("DISTILBERT_CATEGORIZATION_MODEL", DEFAULT_HF_MODEL).strip()
        or DEFAULT_HF_MODEL,
        model_path=Path(model_path).expanduser() if model_path else None,
        allow_download=allow_download,
        local_files_only=_env_bool("DISTILBERT_LOCAL_FILES_ONLY", "true") and not allow_download,
        required=_env_bool("DISTILBERT_REQUIRED", "false"),
        threshold=_env_float("DISTILBERT_CONFIDENCE_THRESHOLD", 0.90),
        batch_size=_env_int("DISTILBERT_BATCH_SIZE", 64),
        shadow=_env_bool("DISTILBERT_SHADOW", "false"),
        use_stub=_env_bool("DISTILBERT_USE_STUB", "false"),
    )


def reset_predictor_cache() -> None:
    """Clear the in-process predictor cache. Intended for tests and CLI smoke runs."""
    _PREDICTOR_CACHE.clear()
    _UNAVAILABLE_WARNED.clear()


def format_distilbert_input(tx: dict[str, Any]) -> tuple[str, list[str]]:
    """Format a Folio transaction for the DistilBERT v2 sign-prefixed input."""
    warnings: list[str] = []
    description = str(
        tx.get("raw_description")
        or tx.get("description")
        or tx.get("merchant_name")
        or ""
    ).strip()
    prefix = "[debit]"
    try:
        amount = float(tx.get("amount"))
        if math.isfinite(amount) and amount > 0:
            prefix = "[credit]"
    except (TypeError, ValueError):
        warnings.append("missing_or_unknown_amount_defaulted_to_debit")
    return f"{prefix} {description}".strip(), warnings


def normalize_model_label(label: Any) -> str:
    text = str(label or "").strip()
    if text.upper().startswith("LABEL_"):
        try:
            index = int(text.split("_", 1)[1])
        except (IndexError, ValueError):
            return text
        if 0 <= index < len(DISTILBERT_LABELS):
            return DISTILBERT_LABELS[index]
        return text
    return re.sub(r"\s+", " ", text.replace("_", " ")).strip().title()


def _normalize_label(label: Any) -> str:
    return re.sub(r"\s+", " ", str(label or "").replace("_", " ").strip().lower())


def _active_category_set(active_categories: list[str] | tuple[str, ...] | set[str]) -> set[str]:
    return {str(category).strip() for category in active_categories if str(category).strip()}


def map_label_to_folio(label: str, *, active_categories: list[str] | tuple[str, ...] | set[str]) -> dict[str, Any]:
    """Map a DistilBERT label to Folio's active taxonomy using a conservative policy."""
    normalized = _normalize_label(label)
    active = _active_category_set(active_categories)

    if normalized in BLOCKED_LABELS:
        return {
            "category": None,
            "status": "ambiguous",
            "reason": f"DistilBERT label {label} needs Folio cashflow context.",
        }

    if normalized in DIRECT_FOLIO_MAPPING:
        category = DIRECT_FOLIO_MAPPING[normalized]
        if category in active:
            return {
                "category": category,
                "status": "mapped",
                "reason": f"DistilBERT label {label} maps directly to {category}.",
            }
        return {
            "category": None,
            "status": "invalid_category",
            "reason": f"Mapped category {category} is not active in Folio.",
        }

    if normalized in CONDITIONAL_FOLIO_MAPPING:
        category = CONDITIONAL_FOLIO_MAPPING[normalized]
        if category in active:
            return {
                "category": category,
                "status": "conditional",
                "reason": f"DistilBERT label {label} conditionally maps to {category}.",
            }
        return {
            "category": None,
            "status": "invalid_category",
            "reason": f"Conditional mapped category {category} is not active in Folio.",
        }

    return {
        "category": None,
        "status": "unknown",
        "reason": f"No Folio mapping exists for DistilBERT label {label}.",
    }


class TransformersDistilbertPredictor:
    def __init__(self, *, config: DistilbertConfig) -> None:
        try:
            from transformers import pipeline
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(f"optional_dependency_missing:transformers:{type(exc).__name__}") from exc

        source = str(config.model_path) if config.model_path else config.model_name
        try:
            self._classifier = pipeline(
                "text-classification",
                model=source,
                tokenizer=source,
                top_k=None,
                local_files_only=config.local_files_only,
            )
        except Exception as exc:  # pragma: no cover - optional dependency/runtime
            raise RuntimeError(f"distilbert_transformers_unavailable:{type(exc).__name__}") from exc
        self.model_id = source

    def predict(self, tx: dict[str, Any]) -> dict[str, Any]:
        input_text, warnings = format_distilbert_input(tx)
        raw = self._classifier(input_text)
        rows = raw[0] if raw and isinstance(raw[0], list) else raw
        ranked = sorted(
            [
                {
                    "label": normalize_model_label(item.get("label")),
                    "score": float(item.get("score") or 0.0),
                }
                for item in (rows or [])
            ],
            key=lambda item: item["score"],
            reverse=True,
        )
        return _prediction_payload(self.model_id, input_text, warnings, ranked)


class ONNXDistilbertPredictor:
    def __init__(self, *, config: DistilbertConfig) -> None:
        try:
            import numpy as np
            import onnxruntime as ort
            from huggingface_hub import snapshot_download
            from transformers import AutoTokenizer
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                f"optional_dependency_missing:onnxruntime_or_transformers:{type(exc).__name__}"
            ) from exc

        if config.model_path is not None:
            model_root = config.model_path
        else:
            try:
                model_root = Path(
                    snapshot_download(
                        config.model_name,
                        allow_patterns=[
                            "config.json",
                            "label_mapping.json",
                            "onnx/model_quantized.onnx",
                            "special_tokens_map.json",
                            "tokenizer.json",
                            "tokenizer_config.json",
                            "vocab.txt",
                        ],
                        local_files_only=config.local_files_only,
                    )
                )
            except Exception as exc:  # pragma: no cover - optional dependency/runtime
                raise RuntimeError(f"distilbert_onnx_unavailable:{type(exc).__name__}") from exc

        onnx_path = model_root / "onnx" / "model_quantized.onnx"
        if not onnx_path.exists():
            raise RuntimeError("distilbert_onnx_unavailable:missing_model_quantized_onnx")

        self.model_id = str(config.model_path or config.model_name)
        self._np = np
        self._session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
        self._input_names = {item.name for item in self._session.get_inputs()}
        self._tokenizer = AutoTokenizer.from_pretrained(str(model_root), local_files_only=True)
        self._id2label = _load_id2label(model_root)

    def predict(self, tx: dict[str, Any]) -> dict[str, Any]:
        input_text, warnings = format_distilbert_input(tx)
        encoded = self._tokenizer(
            input_text,
            return_tensors="np",
            truncation=True,
            max_length=128,
            padding=False,
        )
        inputs = {}
        for key in ("input_ids", "attention_mask", "token_type_ids"):
            if key in encoded and key in self._input_names:
                inputs[key] = encoded[key].astype("int64")
        logits = self._session.run(None, inputs)[0][0]
        probabilities = _softmax(logits, self._np)
        ranked = sorted(
            [
                {"label": self._id2label.get(index, f"LABEL_{index}"), "score": float(probability)}
                for index, probability in enumerate(probabilities)
            ],
            key=lambda item: item["score"],
            reverse=True,
        )
        return _prediction_payload(self.model_id, input_text, warnings, ranked)


class StubDistilbertPredictor:
    """Deterministic offline predictor used by unit tests and eval smoke runs."""

    model_id = "stub-distilbert-us-transaction-classifier-v2"

    def predict(self, tx: dict[str, Any]) -> dict[str, Any]:
        input_text, warnings = format_distilbert_input(tx)
        text = input_text.lower()
        label = "Shopping"
        score = 0.72
        rules = [
            ("refund", "Income", 0.91),
            ("payroll", "Income", 0.97),
            ("salary", "Income", 0.97),
            ("rent", "Rent", 0.95),
            ("mortgage", "Mortgage", 0.95),
            ("market", "Groceries", 0.94),
            ("grocery", "Groceries", 0.94),
            ("cafe", "Restaurants", 0.92),
            ("restaurant", "Restaurants", 0.92),
            ("parking", "Transportation", 0.90),
            ("metro", "Transportation", 0.90),
            ("fuel", "Transportation", 0.90),
            ("clinic", "Healthcare", 0.91),
            ("pharmacy", "Healthcare", 0.91),
            ("insurance", "Insurance", 0.90),
            ("electric", "Utilities", 0.90),
            ("utility", "Utilities", 0.90),
            ("cloud", "Subscription", 0.90),
            ("monthly", "Subscription", 0.88),
            ("cinema", "Entertainment", 0.90),
            ("air", "Travel", 0.89),
            ("course", "Education", 0.88),
            ("salon", "Personal Care", 0.88),
            ("fee", "Fees", 0.91),
            ("transfer", "Transfer", 0.86),
        ]
        for needle, candidate, confidence in rules:
            if needle in text:
                label = candidate
                score = confidence
                break
        ranked = [{"label": label, "score": score}]
        ranked.extend(
            {"label": alt_label, "score": max(0.0, min(1.0, (1.0 - score) / 2))}
            for alt_label in ("Shopping", "Restaurants", "Groceries", "Transportation", "Income")
            if alt_label != label
        )
        return _prediction_payload(self.model_id, input_text, warnings, ranked)


def load_predictor(*, config: DistilbertConfig | None = None) -> DistilbertLoadResult:
    config = config or get_config()
    cached = _PREDICTOR_CACHE.get(config.cache_key)
    if cached is not None:
        return cached

    if config.use_stub:
        result = DistilbertLoadResult(
            predictor=StubDistilbertPredictor(),
            model_id=StubDistilbertPredictor.model_id,
            warnings=("using_stub_distilbert_predictor",),
        )
        _PREDICTOR_CACHE[config.cache_key] = result
        return result

    if config.model_path is not None and not config.model_path.exists():
        result = DistilbertLoadResult(
            predictor=None,
            model_id=str(config.model_path),
            warnings=(f"missing_distilbert_model_path:{config.model_path}",),
        )
        _PREDICTOR_CACHE[config.cache_key] = result
        return result

    warnings: list[str] = []
    try:
        predictor = ONNXDistilbertPredictor(config=config)
        result = DistilbertLoadResult(
            predictor=predictor,
            model_id=predictor.model_id,
            warnings=("using_onnx_distilbert_predictor",),
        )
        _PREDICTOR_CACHE[config.cache_key] = result
        return result
    except RuntimeError as exc:
        warnings.append(str(exc))

    try:
        predictor = TransformersDistilbertPredictor(config=config)
        result = DistilbertLoadResult(
            predictor=predictor,
            model_id=predictor.model_id,
            warnings=tuple(warnings + ["using_transformers_distilbert_predictor"]),
        )
        _PREDICTOR_CACHE[config.cache_key] = result
        return result
    except RuntimeError as exc:
        warnings.append(str(exc))

    result = DistilbertLoadResult(
        predictor=None,
        model_id=str(config.model_path or config.model_name),
        warnings=tuple(warnings),
    )
    _PREDICTOR_CACHE[config.cache_key] = result
    return result


def categorize_batch(
    entries: list[dict[str, Any]],
    *,
    active_categories: list[str],
    threshold: float | None = None,
) -> list[dict[str, Any]]:
    """Categorize model-needed entries and fail closed to Folio suggestions."""
    config = get_config()
    threshold = config.threshold if threshold is None else threshold
    load_result = load_predictor(config=config)
    if not load_result.available:
        _log_unavailable_once(load_result)
        if config.required:
            raise RuntimeError(
                "DistilBERT categorizer is required but unavailable: "
                + "; ".join(load_result.warnings)
            )
        return [
            _fallback_result(i, entry, "model_unavailable", model_id=load_result.model_id)
            for i, entry in enumerate(entries)
        ]

    results: list[dict[str, Any]] = []
    counts = {
        "accepted": 0,
        "shadow": 0,
        "below_threshold": 0,
        "ambiguous": 0,
        "invalid_category": 0,
        "positive_amount": 0,
        "protected_suggestion": 0,
        "prediction_error": 0,
    }
    for i, entry in enumerate(entries):
        tx = entry.get("tx", entry)
        try:
            prediction = load_result.predictor.predict(tx)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            logger.warning("DistilBERT prediction failed for batch index %d: %s", i, exc)
            counts["prediction_error"] += 1
            results.append(_fallback_result(i, entry, "prediction_error", model_id=load_result.model_id))
            continue

        label = normalize_model_label(prediction.get("label"))
        score = float(prediction.get("score") or 0.0)
        mapping = map_label_to_folio(label, active_categories=active_categories)
        base = {
            "index": i,
            "model_id": load_result.model_id,
            "model_score": round(score, 4),
            "external_label": label,
            "mapping_status": mapping["status"],
            "input_warnings": prediction.get("warnings", []),
        }

        if not _is_negative_amount(tx):
            counts["positive_amount"] += 1
            results.append(
                _fallback_result(
                    i,
                    entry,
                    "non_negative_amount_not_auto_applied",
                    model_id=load_result.model_id,
                    extra=base,
                )
            )
            continue

        if score < threshold:
            counts["below_threshold"] += 1
            results.append(
                _fallback_result(
                    i,
                    entry,
                    "below_threshold",
                    model_id=load_result.model_id,
                    extra=base,
                )
            )
            continue

        category = mapping.get("category")
        if not category:
            count_key = "invalid_category" if mapping["status"] == "invalid_category" else "ambiguous"
            counts[count_key] += 1
            results.append(
                _fallback_result(
                    i,
                    entry,
                    mapping["status"],
                    model_id=load_result.model_id,
                    extra=base,
                )
            )
            continue

        suggestion = str(entry.get("suggestion") or "").strip()
        if suggestion in MODEL_PROTECTED_SUGGESTIONS and category != suggestion:
            counts["protected_suggestion"] += 1
            results.append(
                _fallback_result(
                    i,
                    entry,
                    "protected_suggestion",
                    model_id=load_result.model_id,
                    extra=base,
                )
            )
            continue

        if config.shadow:
            counts["shadow"] += 1
            results.append(
                _fallback_result(
                    i,
                    entry,
                    "shadow_mode",
                    model_id=load_result.model_id,
                    extra={**base, "shadow_category": category},
                )
            )
            continue

        counts["accepted"] += 1
        results.append(
            {
                **base,
                "category": category,
                "confidence": _confidence_label(score),
                "categorization_source": "distilbert",
                "accepted": True,
            }
        )

    logger.info(
        "    DistilBERT candidates=%d accepted=%d shadow=%d below_threshold=%d ambiguous=%d invalid=%d protected=%d positive_or_zero=%d errors=%d",
        len(entries),
        counts["accepted"],
        counts["shadow"],
        counts["below_threshold"],
        counts["ambiguous"],
        counts["invalid_category"],
        counts["protected_suggestion"],
        counts["positive_amount"],
        counts["prediction_error"],
    )
    return results


def get_runtime_status(*, preload: bool = False) -> dict[str, Any]:
    config = get_config()
    payload = {
        "backend": "distilbert",
        "modelId": str(config.model_path or config.model_name),
        "threshold": config.threshold,
        "batchSize": config.batch_size,
        "shadow": config.shadow,
        "required": config.required,
        "allowDownload": config.allow_download,
        "localFilesOnly": config.local_files_only,
        "useStub": config.use_stub,
        "loaded": False,
        "available": None,
        "warnings": [],
    }
    cached = _PREDICTOR_CACHE.get(config.cache_key)
    if preload or cached is not None:
        result = load_predictor(config=config)
        payload.update(
            {
                "modelId": result.model_id,
                "loaded": result.available,
                "available": result.available,
                "warnings": list(result.warnings),
            }
        )
    return payload


def _prediction_payload(
    model_id: str,
    input_text: str,
    warnings: list[str],
    ranked: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked = [
        {"label": normalize_model_label(item.get("label")), "score": float(item.get("score") or 0.0)}
        for item in ranked
    ]
    top = ranked[0] if ranked else {"label": "", "score": 0.0}
    return {
        "model_id": model_id,
        "label": top["label"],
        "score": round(float(top["score"]), 4),
        "alternatives": [
            {"label": item["label"], "score": round(float(item["score"]), 4)}
            for item in ranked[1:3]
        ],
        "input_text": input_text,
        "warnings": warnings,
    }


def _fallback_result(
    index: int,
    entry: dict[str, Any],
    reason: str,
    *,
    model_id: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suggestion = entry.get("suggestion")
    if suggestion:
        category = suggestion
        confidence = "rule-medium"
        source = entry.get("suggestion_source") or "rule-medium"
    else:
        category = "Other"
        confidence = "fallback"
        source = "fallback"
    return {
        **(extra or {}),
        "index": index,
        "category": category,
        "confidence": confidence,
        "categorization_source": source,
        "accepted": False,
        "fallback_reason": reason,
        "model_id": model_id,
    }


def _confidence_label(score: float) -> str:
    if score >= 0.90:
        return "high"
    if score >= 0.75:
        return "medium"
    return "low"


def _is_negative_amount(tx: dict[str, Any]) -> bool:
    try:
        amount = float(tx.get("amount"))
    except (TypeError, ValueError):
        return False
    return math.isfinite(amount) and amount < 0


def _log_unavailable_once(load_result: DistilbertLoadResult) -> None:
    key = (load_result.model_id, load_result.warnings)
    if key in _UNAVAILABLE_WARNED:
        return
    _UNAVAILABLE_WARNED.add(key)
    logger.warning(
        "    DistilBERT categorizer unavailable; falling back to rules-only behavior: %s",
        "; ".join(load_result.warnings) or "unknown reason",
    )


def _load_id2label(model_root: Path) -> dict[int, str]:
    for filename in ("label_mapping.json", "config.json"):
        path = model_root / filename
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        id2label = payload.get("id2label") or {}
        if id2label:
            return {int(index): normalize_model_label(label) for index, label in id2label.items()}
    return dict(enumerate(DISTILBERT_LABELS))


def _softmax(values: Any, np: Any) -> Any:
    shifted = values - np.max(values)
    exp_values = np.exp(shifted)
    return exp_values / exp_values.sum()
