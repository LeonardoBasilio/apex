from apex_store.migrate import apply_ddl, migration_files
from fakes import FakeConnection


def test_migration_files_are_sorted_by_filename(tmp_path):
    (tmp_path / "002_second.sql").write_text("-- second")
    (tmp_path / "001_first.sql").write_text("-- first")
    (tmp_path / "010_tenth.sql").write_text("-- tenth")

    files = migration_files(tmp_path)

    assert [f.name for f in files] == ["001_first.sql", "002_second.sql", "010_tenth.sql"]


def test_apply_ddl_executes_and_commits_each_file_in_order(tmp_path):
    (tmp_path / "001_a.sql").write_text("CREATE TABLE a (id INT);")
    (tmp_path / "002_b.sql").write_text("CREATE TABLE b (id INT);")

    connection = FakeConnection()
    applied = apply_ddl(connection, tmp_path)

    assert applied == ["001_a.sql", "002_b.sql"]
    assert [sql for sql, _params in connection.executed] == [
        "CREATE TABLE a (id INT);",
        "CREATE TABLE b (id INT);",
    ]
    assert connection.commit_count == 2


def test_apply_ddl_on_the_real_ddl_directory_applies_stage_events():
    from apex_store.migrate import DDL_DIR

    connection = FakeConnection()
    applied = apply_ddl(connection, DDL_DIR)

    assert "001_stage_events.sql" in applied
    sql, _params = connection.executed[0]
    assert "CREATE TABLE IF NOT EXISTS stage_events" in sql
    assert "PRIMARY KEY (job_id, stage_id, stage_attempt)" in sql
