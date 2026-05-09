from __future__ import annotations

import json
import re
from io import BytesIO
from datetime import date

from pydantic import BaseModel, Field, ValidationError, field_validator

import llm_client


class ParsedReceiptItem(BaseModel):
    raw_name: str = Field(default="")
    normalized_guess: str = Field(default="")
    quantity: float | None = None
    unit: str = Field(default="")
    total_price: float | None = None
    unit_price: float | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("raw_name", "normalized_guess", "unit", mode="before")
    @classmethod
    def _stringify(cls, value):
        return "" if value is None else str(value).strip()


class ParsedReceipt(BaseModel):
    store: str = Field(default="")
    date: str | None = None
    subtotal: float | None = None
    tax: float | None = None
    total: float | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    items: list[ParsedReceiptItem] = Field(default_factory=list)

    @field_validator("store", mode="before")
    @classmethod
    def _store_string(cls, value):
        return "" if value is None else str(value).strip()

    @field_validator("date")
    @classmethod
    def _date_or_none(cls, value):
        if not value:
            return None
        raw = str(value).strip()
        try:
            return date.fromisoformat(raw[:10]).isoformat()
        except ValueError:
            return None


class ReceiptItemUpdate(BaseModel):
    id: int
    raw_item_text: str | None = None
    normalized_item_name: str
    quantity: float | None = None
    unit: str | None = ""
    total_price: float | None = None
    unit_price: float | None = None


class ReceiptDraftMetadataUpdate(BaseModel):
    store_name: str | None = None
    receipt_date: str | None = None

    @field_validator("store_name", mode="before")
    @classmethod
    def _store_string(cls, value):
        return None if value is None else str(value).strip()

    @field_validator("receipt_date", mode="before")
    @classmethod
    def _date_or_none(cls, value):
        if value in {None, ""}:
            return None
        raw = str(value).strip()
        try:
            return date.fromisoformat(raw[:10]).isoformat()
        except ValueError as exc:
            raise ValueError("Receipt date must be YYYY-MM-DD.") from exc


def _normalize_profile(profile: str | None) -> str:
    return (profile or "household").strip() or "household"


def _clean_item_name(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _extract_json_object(text: str) -> dict:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw[start:end + 1])
        raise


def _looks_like_heif(image_bytes: bytes, mime_type: str | None = None) -> bool:
    mime = (mime_type or "").lower()
    if mime in {"image/heic", "image/heif", "image/heic-sequence", "image/heif-sequence"}:
        return True
    # HEIF/HEIC files are ISO BMFF containers with a brand near byte 8.
    header = image_bytes[:32].lower()
    return b"ftypheic" in header or b"ftypheif" in header or b"ftypmif1" in header or b"ftypmsf1" in header


def _prepare_image_for_vision(image_bytes: bytes, mime_type: str | None = None) -> tuple[bytes, str | None]:
    """
    Ollama vision accepts common web image formats but not HEIC/HEIF.
    Convert Apple receipt photos to JPEG before sending them to the model.
    """
    if not _looks_like_heif(image_bytes, mime_type):
        return image_bytes, mime_type

    try:
        from PIL import Image
        import pillow_heif
    except ImportError as exc:
        raise ValueError("HEIC receipts need pillow-heif support. Rebuild the backend image and try again.") from exc

    try:
        pillow_heif.register_heif_opener()
        with Image.open(BytesIO(image_bytes)) as image:
            if image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")
            elif image.mode == "L":
                image = image.convert("RGB")
            output = BytesIO()
            image.save(output, format="JPEG", quality=92, optimize=True)
            return output.getvalue(), "image/jpeg"
    except Exception as exc:
        raise ValueError("Could not convert HEIC receipt. Try exporting the image as JPEG or PNG.") from exc


def parse_receipt_image(image_bytes: bytes, mime_type: str | None = None) -> tuple[ParsedReceipt, str]:
    prompt = """
Extract this grocery receipt into strict JSON only. Do not include markdown.
Use this exact shape:
{
  "store": "Store name",
  "date": "YYYY-MM-DD or null",
  "subtotal": 0.0,
  "tax": 0.0,
  "total": 0.0,
  "confidence": 0.0,
  "items": [
    {
      "raw_name": "receipt text",
      "normalized_guess": "human readable grocery item",
      "quantity": 1.0,
      "unit": "each|lb|oz|gal|pack|",
      "total_price": 0.0,
      "unit_price": 0.0,
      "confidence": 0.0
    }
  ]
}
Rules:
- Include only purchased line items, not payment, loyalty, subtotal, tax, total, coupons, or card lines.
- For weighted produce, use the printed weight as quantity and "lb" when pounds are implied.
- If unit price is missing, compute total_price / quantity when quantity is known.
- Use null for unknown numeric values.
""".strip()
    image_bytes, mime_type = _prepare_image_for_vision(image_bytes, mime_type)
    raw, model = llm_client.complete_vision(
        prompt=prompt,
        image_bytes=image_bytes,
        max_tokens=4096,
        purpose="copilot",
        mime_type=mime_type,
    )
    try:
        parsed = ParsedReceipt.model_validate(_extract_json_object(raw))
    except (ValidationError, json.JSONDecodeError) as exc:
        raise ValueError("Receipt parser returned unreadable JSON. Try a clearer photo.") from exc
    if not parsed.items:
        raise ValueError("No grocery line items were found on this receipt.")
    return parsed, model


def create_draft_receipt(conn, profile: str | None, parsed: ParsedReceipt, parser_model: str) -> dict:
    profile_id = _normalize_profile(profile)
    cursor = conn.execute(
        """
        INSERT INTO receipt_imports
            (profile_id, store_name, receipt_date, subtotal, tax, total, status, parser_model, confidence, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, datetime('now'))
        """,
        (
            profile_id,
            parsed.store,
            parsed.date,
            parsed.subtotal,
            parsed.tax,
            parsed.total,
            parser_model,
            parsed.confidence,
        ),
    )
    receipt_id = cursor.lastrowid
    for item in parsed.items:
        normalized = _clean_item_name(item.normalized_guess or item.raw_name)
        conn.execute(
            """
            INSERT INTO receipt_items
                (receipt_import_id, raw_item_text, normalized_item_name, quantity, unit,
                 total_price, unit_price, confidence, user_corrected, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'))
            """,
            (
                receipt_id,
                item.raw_name,
                normalized,
                item.quantity,
                item.unit,
                item.total_price,
                item.unit_price,
                item.confidence,
            ),
        )
    return get_receipt(conn, receipt_id, profile_id)


def _row_to_receipt(row) -> dict:
    return {
        "id": row["id"],
        "profile_id": row["profile_id"],
        "store_name": row["store_name"],
        "receipt_date": row["receipt_date"],
        "subtotal": row["subtotal"],
        "tax": row["tax"],
        "total": row["total"],
        "status": row["status"],
        "parser_model": row["parser_model"],
        "confidence": row["confidence"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _item_row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "receipt_import_id": row["receipt_import_id"],
        "raw_item_text": row["raw_item_text"],
        "normalized_item_name": row["normalized_item_name"],
        "quantity": row["quantity"],
        "unit": row["unit"],
        "total_price": row["total_price"],
        "unit_price": row["unit_price"],
        "confidence": row["confidence"],
        "user_corrected": bool(row["user_corrected"]),
    }


def get_receipt(conn, receipt_id: int, profile: str | None) -> dict:
    profile_id = _normalize_profile(profile)
    row = conn.execute(
        "SELECT * FROM receipt_imports WHERE id = ? AND profile_id = ?",
        (receipt_id, profile_id),
    ).fetchone()
    if row is None:
        raise KeyError("Receipt not found.")
    receipt = _row_to_receipt(row)
    items = conn.execute(
        "SELECT * FROM receipt_items WHERE receipt_import_id = ? ORDER BY id",
        (receipt_id,),
    ).fetchall()
    receipt["items"] = [_item_row_to_dict(item) for item in items]
    receipt["comparisons"] = comparisons_for_items(conn, profile_id, receipt["items"])
    return receipt


def list_receipts(conn, profile: str | None, statuses: list[str] | None = None, limit: int = 12) -> dict:
    profile_id = _normalize_profile(profile)
    limit = max(1, min(int(limit or 12), 50))
    statuses = [status for status in (statuses or []) if status in {"draft", "approved", "discarded"}]
    params: list = [profile_id]
    status_clause = ""
    if statuses:
        status_clause = f" AND r.status IN ({','.join('?' for _ in statuses)})"
        params.extend(statuses)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT r.*,
               COUNT(i.id) AS item_count
          FROM receipt_imports r
          LEFT JOIN receipt_items i ON i.receipt_import_id = r.id
         WHERE r.profile_id = ?
               {status_clause}
         GROUP BY r.id
         ORDER BY datetime(r.updated_at) DESC, r.id DESC
         LIMIT ?
        """,
        params,
    ).fetchall()
    return {
        "items": [
            {
                **_row_to_receipt(row),
                "item_count": row["item_count"],
            }
            for row in rows
        ]
    }


def update_receipt_items(
    conn,
    receipt_id: int,
    profile: str | None,
    items: list[ReceiptItemUpdate],
    metadata: ReceiptDraftMetadataUpdate | None = None,
) -> dict:
    receipt = get_receipt(conn, receipt_id, profile)
    if receipt["status"] != "draft":
        raise ValueError("Only draft receipts can be edited.")
    if metadata is not None:
        conn.execute(
            """
            UPDATE receipt_imports
               SET store_name = ?,
                   receipt_date = ?,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (
                metadata.store_name or "",
                metadata.receipt_date,
                receipt_id,
            ),
        )
    existing = {
        row["id"]
        for row in conn.execute(
            "SELECT id FROM receipt_items WHERE receipt_import_id = ?",
            (receipt_id,),
        ).fetchall()
    }
    for item in items:
        if item.id not in existing:
            raise KeyError(f"Receipt item {item.id} not found.")
        conn.execute(
            """
            UPDATE receipt_items
               SET raw_item_text = COALESCE(?, raw_item_text),
                   normalized_item_name = ?,
                   quantity = ?,
                   unit = ?,
                   total_price = ?,
                   unit_price = ?,
                   user_corrected = 1,
                   updated_at = datetime('now')
             WHERE id = ? AND receipt_import_id = ?
            """,
            (
                item.raw_item_text,
                _clean_item_name(item.normalized_item_name),
                item.quantity,
                item.unit or "",
                item.total_price,
                item.unit_price,
                item.id,
                receipt_id,
            ),
        )
    conn.execute("UPDATE receipt_imports SET updated_at = datetime('now') WHERE id = ?", (receipt_id,))
    return get_receipt(conn, receipt_id, profile)


def set_receipt_status(conn, receipt_id: int, profile: str | None, status: str) -> dict:
    if status not in {"approved", "discarded"}:
        raise ValueError("Unsupported receipt status.")
    receipt = get_receipt(conn, receipt_id, profile)
    if receipt["status"] == "discarded":
        raise ValueError("Discarded receipts cannot be changed.")
    conn.execute(
        "UPDATE receipt_imports SET status = ?, updated_at = datetime('now') WHERE id = ?",
        (status, receipt_id),
    )
    return get_receipt(conn, receipt_id, profile)


def comparisons_for_items(conn, profile: str, items: list[dict]) -> dict:
    names = sorted({_clean_item_name(item.get("normalized_item_name")) for item in items if item.get("normalized_item_name")})
    if not names:
        return {}
    placeholders = ",".join("?" for _ in names)
    rows = conn.execute(
        f"""
        SELECT i.normalized_item_name,
               r.store_name,
               MIN(i.unit_price) AS lowest_unit_price,
               AVG(i.unit_price) AS average_unit_price,
               COUNT(*) AS sample_count,
               MAX(r.receipt_date) AS latest_receipt_date
          FROM receipt_items i
          JOIN receipt_imports r ON r.id = i.receipt_import_id
         WHERE r.profile_id = ?
           AND r.status = 'approved'
           AND i.normalized_item_name IN ({placeholders})
           AND i.unit_price IS NOT NULL
         GROUP BY i.normalized_item_name, r.store_name
         ORDER BY i.normalized_item_name, lowest_unit_price ASC
        """,
        [profile, *names],
    ).fetchall()
    grouped: dict[str, list[dict]] = {name: [] for name in names}
    for row in rows:
        grouped.setdefault(row["normalized_item_name"], []).append({
            "store_name": row["store_name"],
            "lowest_unit_price": row["lowest_unit_price"],
            "average_unit_price": row["average_unit_price"],
            "sample_count": row["sample_count"],
            "latest_receipt_date": row["latest_receipt_date"],
        })
    return grouped


def get_comparisons(conn, profile: str | None) -> dict:
    profile_id = _normalize_profile(profile)
    rows = conn.execute(
        """
        SELECT i.normalized_item_name,
               r.store_name,
               MIN(i.unit_price) AS lowest_unit_price,
               AVG(i.unit_price) AS average_unit_price,
               COUNT(*) AS sample_count,
               MAX(r.receipt_date) AS latest_receipt_date
          FROM receipt_items i
          JOIN receipt_imports r ON r.id = i.receipt_import_id
         WHERE r.profile_id = ?
           AND r.status = 'approved'
           AND i.unit_price IS NOT NULL
           AND TRIM(i.normalized_item_name) != ''
         GROUP BY i.normalized_item_name, r.store_name
         ORDER BY LOWER(i.normalized_item_name), lowest_unit_price ASC
        """,
        (profile_id,),
    ).fetchall()
    items: dict[str, dict] = {}
    for row in rows:
        name = row["normalized_item_name"]
        item = items.setdefault(name, {"item_name": name, "stores": []})
        item["stores"].append({
            "store_name": row["store_name"],
            "lowest_unit_price": row["lowest_unit_price"],
            "average_unit_price": row["average_unit_price"],
            "sample_count": row["sample_count"],
            "latest_receipt_date": row["latest_receipt_date"],
        })
    return {"items": list(items.values())}
