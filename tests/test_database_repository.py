import duckdb
from pathlib import Path
from data.repositories.database_repository import DatabaseRepository


def test_repository_query_and_indexes(tmp_path):
    db_path = tmp_path / "t.db"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t (id INTEGER, val TEXT)")
    con.execute("INSERT INTO t VALUES (1,'a'),(2,'b')")
    con.close()

    repo = DatabaseRepository(db_path)

    # Execute basic query
    df = repo.execute_query("SELECT * FROM t ORDER BY id", explain=True)
    assert len(df) == 2
    assert list(df["id"]) == [1, 2]

    # Create index and verify
    repo.create_index("t", ["id"])
    with duckdb.connect(str(db_path)) as check:
        idx = check.execute("SELECT index_name FROM duckdb_indexes() WHERE table_name='t'").fetchall()
    assert idx

    plan = repo.explain_query("SELECT * FROM t WHERE id = 1")
    assert not plan.empty

