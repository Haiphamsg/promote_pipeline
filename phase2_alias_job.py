import os, re, csv, time, argparse, unicodedata
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ------------------------
# DB
# ------------------------
def get_conn(prefix: str = "PROD"):
    return psycopg2.connect(
        host=os.environ[f"{prefix}_DB_HOST"],
        dbname=os.environ.get(f"{prefix}_DB_NAME", "postgres"),
        user=os.environ.get(f"{prefix}_DB_USER", "postgres"),
        password=os.environ[f"{prefix}_DB_PASSWORD"],
        port=int(os.environ.get(f"{prefix}_DB_PORT", "5432")),
        sslmode="require",
    )

# ------------------------
# Normalization helpers
# ------------------------
STOPWORDS = {
    "va", "voi", "loai", "moi", "neu", "thi", "dung", "cung", "duoc", "de",
    "it", "mot", "vai", "khoang", "hoac", "cai", "goi", "hop", "chai", "lon",
    "tuoi", "kho", "an", "uong"
}

ABBREV_MAP = {
    # common short forms seen in VN recipes
    "nc": "nuoc",
    "n.c": "nuoc",
    "nuoc": "nuoc",
    "sg": "",  # location noise
    "hn": "",
}

def remove_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", s)

def normalize_key(s: str) -> str:
    """VN key -> ascii-ish token string for similarity comparisons."""
    s = (s or "").strip().lower()
    s = remove_accents(s)
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    tokens = []
    for t in s.split():
        t = ABBREV_MAP.get(t, t)
        if not t or t in STOPWORDS:
            continue
        tokens.append(t)
    return " ".join(tokens)

def similarity(a: str, b: str) -> float:
    """0..1 similarity"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def looks_bad_key(key: str) -> bool:
    """Skip garbage candidates (sentence-like / digit heavy)"""
    if not key:
        return True
    if re.search(r"\d", key):
        return True
    if len(key) >= 40:
        return True
    if len(key.split()) > 8:
        return True
    return False

PACKAGING_WORDS = {
    "hop", "goi", "chai", "lon", "hu", "bi", "tui", "loai", "hop", "chai", "th",
    "lo", "thung", "vien", "que", "mieng"
}

DANGEROUS_NEAR_PAIRS = {
    ("canh", "chanh"), ("chanh", "canh"),
    ("suon", "sun"), ("sun", "suon"),
    ("ga", "gao"), ("gao", "ga"),
    ("ca", "cua"), ("cua", "ca"),
    ("chan", "cha"), ("cha", "chan"),
}

PROTEIN_TOKENS = {"ga", "heo", "bo", "tom", "ca", "cua", "de", "vit", "lon"}

def tokens(n: str) -> List[str]:
    return [t for t in (n or "").split() if t]

def has_packaging_suffix(alias_norm: str, canon_norm: str) -> bool:
    # True if alias == canon + packaging words (only extra packaging tokens)
    ct = tokens(canon_norm)
    at = tokens(alias_norm)
    if not ct or len(at) <= len(ct):
        return False
    if at[:len(ct)] != ct:
        return False
    extra = at[len(ct):]
    return all(t in PACKAGING_WORDS for t in extra)

def is_format_only(canon_norm: str, alias_norm: str) -> bool:
    # same norm => only accents/underscore/case differences
    return canon_norm == alias_norm

def has_dangerous_pair(canon_norm: str, alias_norm: str) -> bool:
    ct = set(tokens(canon_norm))
    at = set(tokens(alias_norm))
    for a, b in DANGEROUS_NEAR_PAIRS:
        if a in ct and b in at:
            return True
    return False

def protein_mismatch(canon_norm: str, alias_norm: str) -> bool:
    ct = set(tokens(canon_norm)) & PROTEIN_TOKENS
    at = set(tokens(alias_norm)) & PROTEIN_TOKENS
    # mismatch if both mention proteins but not the same set
    return bool(ct) and bool(at) and ct != at

def last_token_diff(canon_norm: str, alias_norm: str) -> bool:
    ct = tokens(canon_norm)
    at = tokens(alias_norm)
    if not ct or not at:
        return False
    return ct[-1] != at[-1]

# ------------------------
# Data structures
# ------------------------
@dataclass
class IngredientRow:
    id: str
    key: str
    norm: str
    used_count: int

@dataclass
class Suggestion:
    canonical_id: str
    canonical_key: str
    alias_id: str
    alias_key: str
    score: float
    used_count_canonical: int
    used_count_alias: int
    reason: str

# ------------------------
# Queries
# ------------------------
SQL_INGREDIENTS_WITH_USAGE = """
select
  i.id::text,
  i.key,
  coalesce(u.used_count, 0) as used_count
from ingredients i
left join (
  select ingredient_id, count(*) as used_count
  from recipe_ingredients
  group by ingredient_id
) u on u.ingredient_id = i.id
;
"""

SQL_FIND_ALIAS_NORM_CONFLICTS = """
select alias_norm, count(*)
from ingredient_aliases
group by alias_norm
having count(*) > 1
order by count(*) desc
limit 20;
"""

# ------------------------
# Candidate grouping
# ------------------------
def build_ingredients(conn) -> List[IngredientRow]:
    rows: List[IngredientRow] = []
    with conn.cursor() as cur:
        cur.execute(SQL_INGREDIENTS_WITH_USAGE)
        for iid, key, used_count in cur.fetchall():
            nk = normalize_key(key)
            rows.append(IngredientRow(id=iid, key=key, norm=nk, used_count=int(used_count)))
    return rows

def bucket_key(n: str) -> str:
    """Reduce comparison cost: bucket by first char and length band."""
    if not n:
        return "empty"
    first = n[0]
    ln = len(n)
    band = (ln // 5) * 5  # 0-4, 5-9, 10-14...
    return f"{first}:{band}"

def generate_suggestions(
    ingredients: List[IngredientRow],
    min_score: float = 0.92,
    max_pairs: int = 2000
) -> List[Suggestion]:
    # Filter out obviously bad keys for matching purposes
    clean = [x for x in ingredients if x.norm and not looks_bad_key(x.key)]
    buckets: Dict[str, List[IngredientRow]] = {}
    for ing in clean:
        buckets.setdefault(bucket_key(ing.norm), []).append(ing)

    suggestions: List[Suggestion] = []
    pair_count = 0

    for b, items in buckets.items():
        if len(items) < 2:
            continue

        # sort by usage desc so we prefer popular canonical
        items = sorted(items, key=lambda x: x.used_count, reverse=True)

        # compare within bucket
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                if pair_count >= max_pairs:
                    return suggestions

                a = items[i]
                b2 = items[j]

                # quick rejection: too different lengths
                if abs(len(a.norm) - len(b2.norm)) >= 8:
                    continue

                sc = similarity(a.norm, b2.norm)
                pair_count += 1
                if sc < min_score:
                    continue

                # pick canonical = higher usage; tie -> shorter norm
                if (a.used_count > b2.used_count) or (a.used_count == b2.used_count and len(a.norm) <= len(b2.norm)):
                    canonical, alias = a, b2
                else:
                    canonical, alias = b2, a

                # don't suggest if identical key already
                if canonical.key.strip().lower() == alias.key.strip().lower():
                    continue

                suggestions.append(
                    Suggestion(
                        canonical_id=canonical.id,
                        canonical_key=canonical.key,
                        alias_id=alias.id,
                        alias_key=alias.key,
                        score=sc,
                        used_count_canonical=canonical.used_count,
                        used_count_alias=alias.used_count,
                        reason=f"norm_sim={sc:.3f} bucket={bucket_key(canonical.norm)}"
                    )
                )

    # Sort: higher score first, then higher alias usage (more impact)
    suggestions.sort(key=lambda s: (s.score, s.used_count_alias), reverse=True)
    return suggestions

def export_csv(path: str, suggestions: List[Suggestion], limit: int = 500, approve_packaging: bool = True):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "canonical_id", "canonical_key",
            "alias_id", "alias_key",
            "score", "used_count_canonical", "used_count_alias",
            "reason",
            "decision",
            "approved"
        ])

        for s in suggestions[:limit]:
            canon_norm = normalize_key(s.canonical_key)
            alias_norm = normalize_key(s.alias_key)

            decision = "manual_review"
            approved = ""

            # Auto reject: obvious bad keys or dangerous mismatches
            if looks_bad_key(s.alias_key) or looks_bad_key(s.canonical_key):
                decision = "auto_reject"
            elif protein_mismatch(canon_norm, alias_norm):
                decision = "auto_reject"
            elif has_dangerous_pair(canon_norm, alias_norm):
                decision = "auto_reject"
            elif last_token_diff(canon_norm, alias_norm) and s.score < 0.98:
                # if last token differs and it's not super confident, likely different meaning
                decision = "auto_reject"
            else:
                # Auto approve: format-only (snake_case/diacritics/case)
                if is_format_only(canon_norm, alias_norm):
                    decision = "auto_approve"
                    approved = "Y"
                # Auto approve packaging suffix: "bột chiên giòn" vs "gói bột chiên giòn" AFTER normalization
                elif approve_packaging and has_packaging_suffix(alias_norm, canon_norm):
                    decision = "auto_approve"
                    approved = "Y"
                else:
                    decision = "manual_review"

            w.writerow([
                s.canonical_id, s.canonical_key,
                s.alias_id, s.alias_key,
                f"{s.score:.4f}", s.used_count_canonical, s.used_count_alias,
                s.reason,
                decision,
                approved
            ])

# ------------------------
# Apply (alias + migrate)
# ------------------------
def alias_norm_for_alias(alias_key: str) -> str:
    # use same normalization used earlier for matching; good enough for uniqueness
    return normalize_key(alias_key)

SQL_INSERT_ALIAS = """
insert into ingredient_aliases (ingredient_id, alias, alias_norm)
values (%s, %s, %s)
on conflict do nothing
"""

# Migrate strategy:
# 1) Merge into canonical where both exist for same recipe:
#    - role: core wins
#    - amount/unit: prefer non-null on canonical, else take from alias
#    - note: concat unique-ish (simple)
# 2) Delete alias rows that conflict
# 3) Update remaining alias rows to canonical_id (non-conflicting)
SQL_MERGE_CONFLICTS = """
with conflicts as (
  select
    c.recipe_id,
    c.ingredient_id as canonical_id,
    a.ingredient_id as alias_id,
    c.amount as c_amount, c.unit as c_unit, c.note as c_note, c.role as c_role,
    a.amount as a_amount, a.unit as a_unit, a.note as a_note, a.role as a_role
  from recipe_ingredients c
  join recipe_ingredients a
    on a.recipe_id = c.recipe_id
  where c.ingredient_id = %s
    and a.ingredient_id = %s
),
updated as (
  update recipe_ingredients c
  set
    role = case when c.role = 'core' or conflicts.a_role = 'core' then 'core' else c.role end,
    amount = coalesce(c.amount, conflicts.a_amount),
    unit = coalesce(c.unit, conflicts.a_unit),
    note =
      case
        when c.note is null and conflicts.a_note is null then null
        when c.note is null then conflicts.a_note
        when conflicts.a_note is null then c.note
        else left(c.note || '; ' || conflicts.a_note, 200)
      end
  from conflicts
  where c.recipe_id = conflicts.recipe_id
    and c.ingredient_id = conflicts.canonical_id
  returning c.recipe_id
)
delete from recipe_ingredients a
using conflicts
where a.recipe_id = conflicts.recipe_id
  and a.ingredient_id = conflicts.alias_id
;
"""

SQL_UPDATE_NON_CONFLICT = """
update recipe_ingredients a
set ingredient_id = %s
where a.ingredient_id = %s
  and not exists (
    select 1 from recipe_ingredients c
    where c.recipe_id = a.recipe_id
      and c.ingredient_id = %s
  )
;
"""

def apply_one_pair(conn, canonical_id: str, canonical_key: str, alias_id: str, alias_key: str, dry_run: bool):
    # 1) insert alias record for canonical
    an = alias_norm_for_alias(alias_key)
    if dry_run:
        print(f"[DRY] insert alias: canonical={canonical_key} <- alias='{alias_key}' alias_norm='{an}'")
    else:
        with conn.cursor() as cur:
            cur.execute(SQL_INSERT_ALIAS, (canonical_id, alias_key, an))

    # 2) merge conflicts
    if dry_run:
        print(f"[DRY] merge conflicts + delete dup rows: alias_id={alias_id} -> canonical_id={canonical_id}")
    else:
        with conn.cursor() as cur:
            cur.execute(SQL_MERGE_CONFLICTS, (canonical_id, alias_id))

    # 3) update non-conflicting rows
    if dry_run:
        print(f"[DRY] update non-conflict rows ingredient_id: {alias_id} -> {canonical_id}")
    else:
        with conn.cursor() as cur:
            cur.execute(SQL_UPDATE_NON_CONFLICT, (canonical_id, alias_id, canonical_id))

def load_approved_pairs(csv_path: str) -> List[Tuple[str, str, str, str]]:
    pairs = []
    with open(csv_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            approved = (row.get("approved") or "").strip().lower()
            if approved in ("y", "yes", "1", "true"):
                pairs.append((row["canonical_id"], row["canonical_key"], row["alias_id"], row["alias_key"]))
    return pairs

# ------------------------
# CLI
# ------------------------
def cmd_export(args):
    conn = get_conn("PROD")
    try:
        ings = build_ingredients(conn)
        sugg = generate_suggestions(
            ings,
            min_score=args.min_score,
            max_pairs=args.max_pairs
        )
        export_csv(args.out, sugg, limit=args.limit, approve_packaging=args.approve_packaging)
        print(f"Exported {min(len(sugg), args.limit)} suggestions to {args.out}")
        print("Tip: open CSV, mark approved=Y for pairs you want to apply.")
    finally:
        conn.close()

def cmd_apply(args):
    pairs = load_approved_pairs(args.csv)
    if not pairs:
        print("No approved pairs found in CSV (set approved=Y). Nothing to do.")
        return

    conn = get_conn("PROD")
    try:
        if args.dry_run:
            print(f"Running DRY-RUN apply for {len(pairs)} approved pairs...")
        else:
            print(f"Applying {len(pairs)} approved pairs...")

        # transaction per pair keeps blast radius small
        for idx, (cid, ckey, aid, akey) in enumerate(pairs, 1):
            print(f"\n[{idx}/{len(pairs)}] canonical='{ckey}'  alias='{akey}'  ({aid} -> {cid})")
            if args.dry_run:
                apply_one_pair(conn, cid, ckey, aid, akey, dry_run=True)
                continue

            with conn:
                apply_one_pair(conn, cid, ckey, aid, akey, dry_run=False)

        # optional: show alias_norm conflicts (if any)
        with conn.cursor() as cur:
            cur.execute(SQL_FIND_ALIAS_NORM_CONFLICTS)
            rows = cur.fetchall()
            if rows:
                print("\nWARNING: alias_norm duplicates exist (review if you have unique expectations):")
                for an, cnt in rows:
                    print(" ", an, cnt)
            else:
                print("\nNo alias_norm duplicate groups found (top-20 check).")

        print("\nDone.")
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser("Phase 2.1 alias enrichment job")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ex = sub.add_parser("export", help="Export candidate alias groups to CSV")
    ex.add_argument("--out", default="alias_suggestions.csv")
    ex.add_argument("--min-score", type=float, default=0.92)
    ex.add_argument("--limit", type=int, default=500)
    ex.add_argument("--max-pairs", type=int, default=200000)
    ex.add_argument("--approve-packaging", action="store_true", help="auto-approve packaging suffix like 'goi/hop/loai/chai...'")
    ex.set_defaults(func=cmd_export)

    a = sub.add_parser("apply", help="Apply approved pairs from CSV (alias + migrate)")
    a.add_argument("--csv", default="alias_suggestions.csv")
    a.add_argument("--dry-run", action="store_true")
    a.set_defaults(func=cmd_apply)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
