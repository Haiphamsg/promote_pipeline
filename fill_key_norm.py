import psycopg2
import re
import unicodedata

DSN = "postgresql://postgres:n4iqNQE4n6aBW598@db.cegkonikyrretzgvqnln.supabase.co:5432/postgres"

def norm_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    cur.execute("""
        select id, key
        from ingredients
        where key_norm is null
           or key_norm = '';
    """)
    rows = cur.fetchall()

    print(f"Need normalize: {len(rows)} rows")

    for iid, key in rows:
        kn = norm_text(key)
        cur.execute(
            "update ingredients set key_norm = %s where id = %s",
            (kn, iid)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("DONE")

if __name__ == "__main__":
    main()
