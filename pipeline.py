import os, re, uuid, unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------- Config ----------
NAMESPACE_UUID = uuid.UUID(os.environ.get("NAMESPACE_UUID", "11111111-1111-1111-1111-111111111111"))

def get_conn(prefix: str):
    return psycopg2.connect(
        host=os.environ[f"{prefix}_DB_HOST"],
        dbname=os.environ.get(f"{prefix}_DB_NAME", "postgres"),
        user=os.environ.get(f"{prefix}_DB_USER", "postgres"),
        password=os.environ[f"{prefix}_DB_PASSWORD"],
        port=int(os.environ.get(f"{prefix}_DB_PORT", "5432")),
        sslmode="require",
    )

# ---------- Ingredient parsing (Phase 1) ----------
UNIT_MAP = {
  "g": "g", "gram": "g", "gam": "g",
  "kg": "kg",
  "ml": "ml",
  "l": "l", "lit": "l", "lít": "l",
  "tbsp": "tbsp", "tsp": "tsp",
  "thìa": "thìa", "muỗng": "muỗng",
  "củ": "củ", "quả": "quả", "lá": "lá", "nhánh": "nhánh", "tép": "tép",
  "miếng": "miếng", "khoanh": "khoanh",
  "gói": "gói", "hộp": "hộp", "chai": "chai", "lon": "lon",
  "bát": "bát", "chén": "chén",
}
UNIT_PATTERN = r"(?:kg|g|gram|gam|ml|l|lít|lit|tbsp|tsp|thìa|muỗng|củ|quả|lá|nhánh|tép|miếng|khoanh|gói|hộp|chai|lon|bát|chén)"

RE_BULLET = re.compile(r"^[\s\-\•\*\+\·]+")
RE_SPACES = re.compile(r"\s+")
RE_PARENS = re.compile(r"\(([^)]{1,200})\)")

RE_FRACTION_UNIT = re.compile(rf"^(?P<num>\d+)\s*/\s*(?P<den>\d+)\s*(?P<unit>{UNIT_PATTERN})\b\s*(?P<rest>.*)$", re.IGNORECASE)
RE_DECIMAL_UNIT  = re.compile(rf"^(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>{UNIT_PATTERN})\b\s*(?P<rest>.*)$", re.IGNORECASE)

RE_NOTE_PHRASES = re.compile(r"\b(vừa\s*đủ|tùy\s*thích|tuỳ\s*thích|tùy|tuỳ|ít|một\s*ít|nếu\s*thích|không\s*bắt\s*buộc|có\s*thể)\b", re.IGNORECASE)
RE_SUFFIX_MOD = re.compile(r"^(?P<name>.*?)(?:\s+(?P<mod>nhỏ|to|vừa|lớn|tươi|khô|băm\s*nhỏ|băm|thái\s*nhỏ|thái|xắt|cắt\s*lát|cắt\s*nhỏ|cắt|xay|giã|đập\s*dập|rửa\s*sạch|gọt\s*vỏ|bỏ\s*vỏ|bỏ\s*hạt))$", re.IGNORECASE)

def normalize_spaces(s: str) -> str:
    return RE_SPACES.sub(" ", s).strip()

def remove_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", s)

def normalize_alias_norm(key: str) -> str:
    s = remove_accents(key.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return normalize_spaces(s)

def preclean_extract_parentheses(raw: str) -> Tuple[str, Optional[str]]:
    s = normalize_spaces(RE_BULLET.sub("", (raw or "").strip()))
    s = s.replace(";", ",")
    notes = RE_PARENS.findall(s)
    if notes:
        s = normalize_spaces(RE_PARENS.sub("", s))
        note = "; ".join(normalize_spaces(n) for n in notes if n.strip())
        return s, (note or None)
    return s, None

def parse_decimal(val: str) -> Optional[Decimal]:
    try:
        return Decimal(val.replace(",", "."))
    except (InvalidOperation, AttributeError):
        return None

def normalize_unit(u: str) -> Optional[str]:
    if not u:
        return None
    return UNIT_MAP.get(u.strip().lower(), u.strip().lower())

def split_combo(text_main: str) -> List[str]:
    if "," not in text_main:
        return [text_main]
    parts = [normalize_spaces(p) for p in text_main.split(",")]
    return [p for p in parts if p]

def parse_amount_unit_minimal(text_main: str) -> Tuple[Optional[Decimal], Optional[str], str]:
    s = text_main.strip()
    m = RE_FRACTION_UNIT.match(s)
    if m:
        num = Decimal(m.group("num"))
        den = Decimal(m.group("den"))
        unit = normalize_unit(m.group("unit"))
        rest = normalize_spaces(m.group("rest"))
        return (num / den if den != 0 else None), unit, rest

    m = RE_DECIMAL_UNIT.match(s)
    if m:
        amount = parse_decimal(m.group("val"))
        unit = normalize_unit(m.group("unit"))
        rest = normalize_spaces(m.group("rest"))
        return amount, unit, rest

    return None, None, s

def extract_key_and_note(rest: str, note0: Optional[str]) -> Tuple[str, Optional[str]]:
    s = normalize_spaces(rest)
    note_parts: List[str] = []
    if note0:
        note_parts.append(note0)

    phrases = RE_NOTE_PHRASES.findall(s)
    if phrases:
        for ph in phrases:
            phn = normalize_spaces(ph)
            if phn and phn not in note_parts:
                note_parts.append(phn)
        s = normalize_spaces(RE_NOTE_PHRASES.sub("", s))

    m = RE_SUFFIX_MOD.match(s)
    if m:
        name = normalize_spaces(m.group("name"))
        mod = normalize_spaces(m.group("mod"))
        if mod:
            note_parts.append(mod)
        s = name

    s = normalize_spaces(s.strip(" .:-–—"))
    key = s.lower()
    note = "; ".join([p for p in note_parts if p]) if note_parts else None
    return key, note

def infer_role(raw: str, note: Optional[str], is_combo: bool) -> str:
    if is_combo:
        return "optional"
    if RE_NOTE_PHRASES.search((raw or "").lower()):
        return "optional"
    if note and RE_NOTE_PHRASES.search(note.lower()):
        return "optional"
    return "core"

@dataclass
class ParsedIngredient:
    amount: Optional[Decimal]
    unit: Optional[str]
    key: str
    alias_norm: str
    note: Optional[str]
    role: str  # core|optional

def parse_ingredient_text_phase1(raw_text: str) -> List[ParsedIngredient]:
    text_main, note0 = preclean_extract_parentheses(raw_text)
    parts = split_combo(text_main)
    is_combo = len(parts) > 1
    out: List[ParsedIngredient] = []

    for part in parts:
        if not part:
            continue
        amount, unit, rest = parse_amount_unit_minimal(part)
        key, note = extract_key_and_note(rest, note0)
        if not key:
            continue
        # Ingredient key không bao giờ có số
        if re.search(r"\d", key):
            continue
        alias_norm = normalize_alias_norm(key)
        role = infer_role(raw_text, note, is_combo=is_combo)
        out.append(ParsedIngredient(amount, unit, key, alias_norm, note, role))
    return out

# ---------- Recipe deterministic UUID ----------
def recipe_uuid(source: str, locale: str, recipe_id: int) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE_UUID, f"{source}:{locale}:{recipe_id}")

def slugify(name: str) -> str:
    s = remove_accents((name or "").lower())
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "recipe"

def shorten(text: Optional[str], n: int = 240) -> Optional[str]:
    if not text:
        return None
    t = normalize_spaces(text)
    return (t[:n-1] + "…") if len(t) > n else t

# ---------- STG reads ----------
def load_stg_recipe(stg_cur, stg_recipe_id: int):
    stg_cur.execute("""
        SELECT recipe_id, source, locale, name, description, hero_image
        FROM stg_recipes
        WHERE recipe_id = %s
    """, (stg_recipe_id,))
    return stg_cur.fetchone()

def load_stg_steps(stg_cur, stg_recipe_id: int):
    stg_cur.execute("""
        SELECT step_index, step_text
        FROM stg_recipe_steps
        WHERE recipe_id = %s
        ORDER BY step_index
    """, (stg_recipe_id,))
    return stg_cur.fetchall()

def load_stg_ingredients(stg_cur, stg_recipe_id: int):
    stg_cur.execute("""
        SELECT ingredient_text
        FROM stg_recipe_ingredients
        WHERE recipe_id = %s
        ORDER BY ingredient_index
    """, (stg_recipe_id,))
    return [r[0] for r in stg_cur.fetchall()]

def list_recent_stg_ids(stg_conn, limit=20):
    with stg_conn.cursor() as cur:
        cur.execute("""
            SELECT recipe_id
            FROM stg_recipes
            ORDER BY recipe_id DESC
            LIMIT %s
        """, (limit,))
        return [r[0] for r in cur.fetchall()]


# ---------- PROD writes ----------
def upsert_product_recipe(prod_cur, rid_uuid, name, slug, image_url, short_note):
    # DEFAULTS - CHỐT THEO SCHEMA THỰC TẾ
    default_tag = "weekday"          # recipe_tag enum
    default_category = "weekday"     # TEXT nhưng FE expect
    default_cook_time = 15
    default_difficulty = "easy"

    prod_cur.execute("""
        INSERT INTO recipes (
            id,
            name,
            slug,
            tag,
            category,
            cook_time_minutes,
            difficulty,
            image_url,
            short_note,
            is_active
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            slug = EXCLUDED.slug,

            -- KHÔNG ghi đè nếu product đã có dữ liệu chuẩn
            tag = COALESCE(recipes.tag, EXCLUDED.tag),
            category = COALESCE(recipes.category, EXCLUDED.category),
            cook_time_minutes = COALESCE(recipes.cook_time_minutes, EXCLUDED.cook_time_minutes),
            difficulty = COALESCE(recipes.difficulty, EXCLUDED.difficulty),

            image_url = COALESCE(EXCLUDED.image_url, recipes.image_url),
            short_note = COALESCE(EXCLUDED.short_note, recipes.short_note)
    """, (
        str(rid_uuid),
        name,
        slug,
        default_tag,
        default_category,
        default_cook_time,
        default_difficulty,
        image_url,
        short_note
    ))


def replace_recipe_steps(prod_cur, rid_uuid, steps_rows):
    prod_cur.execute("DELETE FROM recipe_steps WHERE recipe_id = %s", (str(rid_uuid),))
    if not steps_rows:
        return
    values = []
    for step_index, step_text in steps_rows:
        step_no = int(step_index) + 1
        values.append((str(rid_uuid), step_no, step_text, None))
    execute_values(prod_cur, """
        INSERT INTO recipe_steps (recipe_id, step_no, content, tip)
        VALUES %s
    """, values)

def get_or_create_ingredient(prod_cur, key: str, alias_norm: str) -> str:
    # 0) chuẩn hoá nhẹ để match đúng unique key
    key = (key or "").strip().lower()
    if not key:
        raise ValueError("Empty ingredient key")

    # 1) lookup alias_norm trước (nhanh nhất)
    prod_cur.execute("""
        SELECT ingredient_id
        FROM ingredient_aliases
        WHERE alias_norm = %s
        LIMIT 1
    """, (alias_norm,))
    row = prod_cur.fetchone()
    if row:
        return row[0]

    # 2) lookup theo ingredients.key (vì key UNIQUE)
    prod_cur.execute("""
        SELECT id
        FROM ingredients
        WHERE key = %s
        LIMIT 1
    """, (key,))
    row = prod_cur.fetchone()
    if row:
        ingredient_id = row[0]
        # ensure alias exists (không fail nếu đã có)
        prod_cur.execute("""
            INSERT INTO ingredient_aliases (ingredient_id, alias, alias_norm)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (ingredient_id, key, alias_norm))
        return ingredient_id

    # 3) create ingredient (an toàn với UNIQUE)
    prod_cur.execute("""
        INSERT INTO ingredients (
            key,
            display_name,
            search_text,
            "group",
            is_core_default
        )
        VALUES (%s, %s, %s, 'other', FALSE)
        ON CONFLICT (key) DO UPDATE SET
            key = EXCLUDED.key
        RETURNING id
    """, (key, key, alias_norm))
    ingredient_id = prod_cur.fetchone()[0]

    # 4) create alias (không fail nếu trùng)
    prod_cur.execute("""
        INSERT INTO ingredient_aliases (ingredient_id, alias, alias_norm)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (ingredient_id, key, alias_norm))

    return ingredient_id

def replace_recipe_ingredients(prod_cur, rid_uuid, parsed_items):
    prod_cur.execute("DELETE FROM recipe_ingredients WHERE recipe_id = %s", (str(rid_uuid),))
    if not parsed_items:
        return
    values = [(str(rid_uuid), ing_id, amt, unit, note, role) for (ing_id, amt, unit, note, role) in parsed_items]
    execute_values(prod_cur, """
        INSERT INTO recipe_ingredients (recipe_id, ingredient_id, amount, unit, note, role)
        VALUES %s
    """, values)

def set_recipe_active(prod_cur, rid_uuid, active: bool):
    prod_cur.execute("UPDATE recipes SET is_active = %s WHERE id = %s", (active, str(rid_uuid)))

# ---------- Orchestrator ----------
def promote_recipe(stg_conn, prod_conn, stg_recipe_id: int):
    def merge_note(a: Optional[str], b: Optional[str], max_len: int = 200) -> Optional[str]:
        parts = []
        for x in [a, b]:
            if x:
                parts.extend([p.strip() for p in x.split(";") if p.strip()])
        seen = set()
        uniq = []
        for p in parts:
            pl = p.lower()
            if pl not in seen:
                seen.add(pl)
                uniq.append(p)
        out = "; ".join(uniq)
        if not out:
            return None
        return out[:max_len]

    def pick_role(old_role: str, new_role: str) -> str:
        if old_role == "core" or new_role == "core":
            return "core"
        return "optional"

    def has_amount(amount) -> bool:
        return amount is not None

    with stg_conn.cursor() as scur:
        r = load_stg_recipe(scur, stg_recipe_id)
        if not r:
            return {"status": "skip", "recipe_id": stg_recipe_id, "reason": "not found"}
        recipe_id, source, locale, name, desc, hero = r
        steps = load_stg_steps(scur, stg_recipe_id)
        ing_texts = load_stg_ingredients(scur, stg_recipe_id)

    rid_uuid = recipe_uuid(source or "cookpad", locale or "vi", int(recipe_id))
    pname = name or f"Recipe {recipe_id}"
    slug = f"{slugify(pname)}-{recipe_id}"
    short_note = shorten(desc)

    with prod_conn:
        with prod_conn.cursor() as pcur:
            upsert_product_recipe(pcur, rid_uuid, pname, slug, hero, short_note)
            replace_recipe_steps(pcur, rid_uuid, steps)

            dedup: dict[str, tuple] = {}  # ing_id -> (ing_id, amount, unit, note, role)
            for raw in ing_texts:
                raw = strip_bad_prefix(raw)
                if looks_like_sentence(raw):
                    continue
                if not raw or not raw.strip():
                    continue
                for pi in parse_ingredient_text_phase1(raw):
                    ing_id = get_or_create_ingredient(pcur, pi.key, pi.alias_norm)
                    ing_key = str(ing_id)
                    if ing_key not in dedup:
                        dedup[ing_key] = (ing_id, pi.amount, pi.unit, pi.note, pi.role)
                    else:
                        _, amt0, unit0, note0, role0 = dedup[ing_key]
                        role = pick_role(role0, pi.role)

                        if has_amount(amt0):
                            amt, unit = amt0, unit0
                        elif has_amount(pi.amount):
                            amt, unit = pi.amount, pi.unit
                        else:
                            amt, unit = None, None

                        note = merge_note(note0, pi.note)
                        dedup[ing_key] = (ing_id, amt, unit, note, role)

            parsed_rows = list(dedup.values())
            replace_recipe_ingredients(pcur, rid_uuid, parsed_rows)

            active = (len(steps) >= 3 and len(parsed_rows) >= 4 and len(name or "") >= 10)
            set_recipe_active(pcur, rid_uuid, active)

    return {"status": "ok", "stg_recipe_id": stg_recipe_id, "product_recipe_id": str(rid_uuid), "steps": len(steps), "ingredients": len(parsed_rows), "active": active}

def looks_like_sentence(text: str) -> bool:
    words = text.split()
    return len(words) > 8

def run_batch(hours=48, limit=3000):
    stg_conn = get_conn("STG")
    prod_conn = get_conn("PROD")
    try:
        ids = list_recent_stg_ids(stg_conn, limit=limit)
        ok = skip = fail = 0
        for rid in ids:
            try:
                out = promote_recipe(stg_conn, prod_conn, int(rid))
                if out["status"] == "ok":
                    ok += 1
                else:
                    skip += 1
            except Exception as e:
                fail += 1
                print("FAIL", rid, e)
        return {"ok": ok, "skip": skip, "fail": fail, "total": len(ids)}
    finally:
        stg_conn.close()
        prod_conn.close()

BAD_PREFIXES = [
  "mình có",
  "bạn nào",
  "ai có",
  "nhà mình",
  "ở sg",
  "ở hn",
  "mình ở",
]

def strip_bad_prefix(text: str) -> str:
    t = text.lower()
    for p in BAD_PREFIXES:
        if t.startswith(p):
            return text[len(p):].strip()
    return text

if __name__ == "__main__":
    print(run_batch(limit=1000))

