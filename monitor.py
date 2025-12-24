import os, time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn(prefix: str):
    return psycopg2.connect(
        host=os.environ[f"{prefix}_DB_HOST"],
        dbname=os.environ.get(f"{prefix}_DB_NAME", "postgres"),
        user=os.environ.get(f"{prefix}_DB_USER", "postgres"),
        password=os.environ[f"{prefix}_DB_PASSWORD"],
        port=int(os.environ.get(f"{prefix}_DB_PORT", "5432")),
        sslmode="require",
    )

SQL = """
select
  now() as asof,
  (select count(*) from recipes) as recipes_total,
  (select count(*) from recipes where is_active) as recipes_active,
  (select count(*) from recipes where not is_active) as recipes_inactive,
  (select count(*) from recipes where created_at >= now() - interval '24 hours') as recipes_24h,
  (select count(*) from ingredients) as ingredients_total,
  (select count(*) from ingredients where created_at >= now() - interval '24 hours') as ingredients_24h,
  (select count(*) from ingredients where key ~ '[0-9]') as ing_key_has_digit,
  (select count(*) from ingredients where length(key) >= 40) as ing_key_too_long,
  (select count(*) from recipe_ingredients) as recipe_ingredients_rows
;
"""

def clear():
    print("\033[2J\033[H", end="")

def main(interval=10):
    conn = get_conn("PROD")
    try:
        while True:
            with conn.cursor() as cur:
                cur.execute(SQL)
                row = cur.fetchone()

            clear()
            (asof, recipes_total, recipes_active, recipes_inactive,
             recipes_24h, ingredients_total, ingredients_24h,
             ing_key_has_digit, ing_key_too_long, ri_rows) = row

            print("=== PRODUCT METRICS (realtime) ===")
            print("asof:", asof)
            print(f"recipes_total: {recipes_total}")
            print(f"recipes_active: {recipes_active}")
            print(f"recipes_inactive: {recipes_inactive}")
            print(f"recipes_created_24h: {recipes_24h}")
            print("---")
            print(f"ingredients_total: {ingredients_total}")
            print(f"ingredients_created_24h: {ingredients_24h}")
            print(f"ingredients_key_has_digit: {ing_key_has_digit}")
            print(f"ingredients_key_too_long(>=40): {ing_key_too_long}")
            print("---")
            print(f"recipe_ingredients_rows: {ri_rows}")
            print("===============================")

            time.sleep(interval)
    finally:
        conn.close()

if __name__ == "__main__":
    main(interval=10)
