import os

from nexus_n3.admin.app import _list_log_names_newest_first, _read_log_tail_newest_first


def test_logs_and_log_lines_are_newest_first(tmp_path):
    older_log = tmp_path / "older.log"
    newer_log = tmp_path / "newer.log"
    older_log.write_text("oldest entry\nmiddle entry\nlatest entry\n", encoding="utf-8")
    newer_log.write_text("new file\n", encoding="utf-8")
    os.utime(older_log, (100, 100))
    os.utime(newer_log, (200, 200))
    assert _list_log_names_newest_first(tmp_path) == ["newer.log", "older.log"]
    assert _read_log_tail_newest_first(older_log, 200) == [
        "latest entry\n",
        "middle entry\n",
        "oldest entry\n",
    ]
