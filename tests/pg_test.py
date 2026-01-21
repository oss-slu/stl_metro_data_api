"""
PostgreSQL CRUD Test.
Verifies Create, Read, Update, and Delete operations on snapshots and historic_data tables.
Structured to match basic_test.py style with try/except and terminal-friendly output.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import pytest

pytestmark = pytest.mark.integration

# Load environment variables
try: 
    load_dotenv()
except ImportError:
    pass

PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', '123456')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')


def get_conn():
    """Get PostgreSQL connection using psycopg2."""
    return psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

def test_postgresql_crud():
    """Test PostgreSQL CRUD operations on snapshots and historic_data tables."""
    print("Testing PostgreSQL CRUD operations...")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- Create ---
    cur.execute("""
        INSERT INTO snapshots (id, data)
        VALUES (DEFAULT, %s)
        RETURNING id, data;
    """, ('{"message": "test snapshot"}',))
    snapshot = cur.fetchone()
    print("Create: Inserted snapshot:", snapshot)
    assert snapshot is not None

    cur.execute("""
        INSERT INTO historic_data (snapshot_id, old_data)
        VALUES (%s, %s)
        RETURNING id, snapshot_id, old_data;
    """, (snapshot["id"], '{"message": "test historic"}'))
    historic = cur.fetchone()
    print("Create: Inserted historic:", historic)
    assert historic is not None
    conn.commit()

    # --- Read ---
    cur.execute("SELECT * FROM snapshots LIMIT 5;")
    snapshots_sample = cur.fetchall()
    print("Read: Snapshots sample:", snapshots_sample)
    assert len(snapshots_sample) > 0

    cur.execute("SELECT * FROM historic_data LIMIT 5;")
    historic_sample = cur.fetchall()
    print("Read: Historic_data sample:", historic_sample)
    assert len(historic_sample) > 0

    # --- Update ---
    cur.execute("""
        UPDATE snapshots 
        SET data = %s 
        WHERE id = %s 
        RETURNING id, data;
    """, ('{"message": "updated snapshot"}', snapshot["id"]))
    updated = cur.fetchone()
    print("Update: Updated snapshot:", updated)
    assert updated["data"]["message"] == "updated snapshot"

    cur.execute("""
        UPDATE historic_data
        SET old_data = %s
        WHERE id = %s
        RETURNING id, snapshot_id, old_data;
        """, ('{"message": "updated historic"}', historic["id"])
    )
    updated_historic = cur.fetchone()
    print("Updated historic_data:", updated_historic)
    assert updated_historic["old_data"]["message"] == "updated historic"

    conn.commit()

    # --- Delete ---
    cur.execute("DELETE FROM historic_data WHERE snapshot_id = %s;", (snapshot["id"],))
    cur.execute("DELETE FROM snapshots WHERE id = %s;", (snapshot["id"],))
    conn.commit()
    print("Delete: Cleaned up inserted test rows.")

    # --- Cleanup safety: reset tables for consistent reruns ---
    cur.execute("TRUNCATE historic_data, snapshots RESTART IDENTITY CASCADE;")
    conn.commit()

    cur.close()
    conn.close()

    print("PostgreSQL CRUD: OK")

def main():
    """Run CRUD test in PostgreSQL."""
    print("PostgreSQL CRUD Test")
    print("=" * 30)

    try:
        test_postgresql_crud()
        print("\nResults:")
        print("PostgreSQL CRUD: PASS")
        print("\nAll PostgreSQL CRUD operations passed!")
        return 0
    except Exception as e:
        print("\nResults:")
        print("PostgreSQL CRUD: FAIL")
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
