import csv
import argparse
from dataclasses import dataclass
from typing import List, Dict, Tuple

import psycopg2
from psycopg2 import errors


@dataclass
class Row:
    alias_norm: str
    canonical_id: str
    canonical_key: str
    decision: str
    notes: str


SQL_OTHER_IDS_FOR_ALIAS = """
select distinct ingredient_id
from ingredient_aliases
where alias_norm = %s
  and ingredient_id <> %s
order by ingredient_id;
"""

# IMPORTANT:
# - recipe_ingredients has unique (recipe_id, ingredient_id)
# - We must avoid duplicate pairs when migrating.
# So we do: delete conflicts first, then update.
SQL_DELETE_CONFLICTS = """
delete from recipe_ingredients ri
where ri.ingredient_id = %s
  and exists (
    select 1 from recipe_ingredients r2
    where r2.recipe_id = ri.recipe_id
      and r2.ingredient_id = %s
  );
"""

SQL_UPDATE_RECIPE_INGREDIENTS = """
update recipe_ingredients
set ingredient_id = %s
where ingredient_id = %s;
"""

SQL_INSERT_ALIASES_TO_CANONICAL = """
insert into ingredient_aliases (ingredient_id, alias, alias_norm)
select
    %s as ingredient_id,
    ia.alias,
    ia.alias_norm
from ingredient_aliases ia
where ia.ingredient_id = %s
  and ia.alias_norm = %s
  and not exists (
      select 1
      from ingredient_aliases x
      where x.ingredient_id = %s
        and x.alias_norm = ia.alias_norm
  );
"""

SQL_DELETE_ALIASES_FROM_OTHER = """
delete from ingredient_aliases
where ingredient_id = %s
  and alias_norm = %s;
"""

SQL_DELETE_INGREDIENT_IF_ORPHAN = """
delete from ingredients i
where i.id = %s
  and not exists (select 1 from recipe_ingredients ri where ri.ingredient_id = i.id)
  and not exists (select 1 from ingredient_aliases ia where ia.ingredient_id = i.id);
"""


def read_csv(path: str) -> List[Row]:
    out: List[Row] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            out.append(Row(
                alias_norm=(r.get("alias_norm") or "").strip(),
                canonical_id=(r.get("canonical_id") or "").strip(),
                canonical_key=(r.get("canonical_key") or "").strip(),
                decision=(r.get("decision") or "").strip().lower(),
                notes=(r.get("notes") or "").strip(),
            ))
    return out


def apply_one_alias(conn, alias_norm: str, canonical_id: str, dry_run: bool) -> Dict[str, int]:
    stats = {"aliases": 0, "ri_conflicts_deleted": 0, "ri_updated": 0, "ingredients_deleted": 0}

    with conn.cursor() as cur:
        cur.execute(SQL_OTHER_IDS_FOR_ALIAS, (alias_norm, canonical_id))
        other_ids = [row[0] for row in cur.fetchall()]

        for other_id in other_ids:
            # 1) remove conflicts to avoid unique violation
            if not dry_run:
                cur.execute(SQL_DELETE_CONFLICTS, (other_id, canonical_id))
                stats["ri_conflicts_deleted"] += cur.rowcount
            else:
                # estimate conflicts count
                cur.execute("""
                    select count(*)
                    from recipe_ingredients ri
                    where ri.ingredient_id = %s
                      and exists (
                        select 1 from recipe_ingredients r2
                        where r2.recipe_id = ri.recipe_id
                          and r2.ingredient_id = %s
                      )
                """, (other_id, canonical_id))
                stats["ri_conflicts_deleted"] += cur.fetchone()[0]

            # 2) migrate recipe_ingredients
            if not dry_run:
                cur.execute(SQL_UPDATE_RECIPE_INGREDIENTS, (canonical_id, other_id))
                stats["ri_updated"] += cur.rowcount
            else:
                cur.execute("select count(*) from recipe_ingredients where ingredient_id = %s", (other_id,))
                stats["ri_updated"] += cur.fetchone()[0]

            # 3) repoint aliases of this alias_norm
            if not dry_run:
                cur.execute(
                    SQL_INSERT_ALIASES_TO_CANONICAL,
                    (canonical_id, other_id, alias_norm, canonical_id)
                )
                # rowcount for INSERT with DO NOTHING isn't always reliable, ok to ignore
                cur.execute(
                    SQL_DELETE_ALIASES_FROM_OTHER,
                    (other_id, alias_norm)
                )
                stats["aliases"] += cur.rowcount
            else:
                cur.execute(
                    "select count(*) from ingredient_aliases where ingredient_id = %s and alias_norm = %s",
                    (other_id, alias_norm)
                )
                stats["aliases"] += cur.fetchone()[0]

            # 4) delete orphan ingredient if no longer referenced
            if not dry_run:
                cur.execute(SQL_DELETE_INGREDIENT_IF_ORPHAN, (other_id,))
                stats["ingredients_deleted"] += cur.rowcount
            else:
                # dry check
                cur.execute("""
                    select
                      (not exists (select 1 from recipe_ingredients where ingredient_id = %s))
                      and (not exists (select 1 from ingredient_aliases where ingredient_id = %s))
                """, (other_id, other_id))
                if cur.fetchone()[0]:
                    stats["ingredients_deleted"] += 1

    return stats


def run_apply(conn, rows: List[Row], dry_run: bool) -> Dict[str, int]:
    total = {"alias_norms": 0, "skipped_manual": 0, "aliases": 0, "ri_conflicts_deleted": 0, "ri_updated": 0, "ingredients_deleted": 0}
    for idx, r in enumerate(rows, 1):
        if not r.alias_norm or not r.canonical_id:
            continue

        if r.decision != "auto":
            total["skipped_manual"] += 1
            continue

        total["alias_norms"] += 1

        # transaction per alias_norm (safer)
        for attempt in range(3):
            try:
                with conn:
                    stats = apply_one_alias(conn, r.alias_norm, r.canonical_id, dry_run=dry_run)
                break
            except errors.UniqueViolation:
                conn.rollback()
                if attempt == 2:
                    raise
        total["aliases"] += stats["aliases"]
        total["ri_conflicts_deleted"] += stats["ri_conflicts_deleted"]
        total["ri_updated"] += stats["ri_updated"]
        total["ingredients_deleted"] += stats["ingredients_deleted"]

        print(f"[{idx}/{len(rows)}] alias_norm='{r.alias_norm}' -> canonical='{r.canonical_key}' ({r.canonical_id}) "
              f"ri_upd={stats['ri_updated']} del_conf={stats['ri_conflicts_deleted']} alias_upd={stats['aliases']} del_ing={stats['ingredients_deleted']}")

    return total


def main():
    ap = argparse.ArgumentParser("Resolve alias_norm collisions")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_apply = sub.add_parser("apply")
    ap_apply.add_argument("--csv", required=True)
    ap_apply.add_argument("--dry-run", action="store_true")

    ap_apply.add_argument("--dsn", required=True, help="Postgres DSN to PRODUCT db")

    args = ap.parse_args()

    rows = read_csv(args.csv)

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False

    totals = run_apply(conn, rows, dry_run=args.dry_run)
    print("=== TOTAL ===")
    print(totals)


if __name__ == "__main__":
    main()
