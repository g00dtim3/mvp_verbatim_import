"""
tests/test_reset_verbatims.py
Tests unitaires pour scripts/reset_verbatims.py (spec §1.4, §1.5).

Le script est destiné à un usage opérationnel contre une vraie base
(dump pg_dump inclus) — ces tests vérifient uniquement la logique de
sécurité (dry-run, confirmation) avec une connexion mockée, sans jamais
appeler pg_dump ni une vraie base.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import scripts.reset_verbatims as reset_mod  # noqa: E402


def _make_conn(db_name="compass_test", n_verbatims=42):
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    # Deux appels attendus en amont de toute purge : current_database(), COUNT(*)
    cur.fetchone.side_effect = [(db_name,), (n_verbatims,)]

    conn = MagicMock()
    conn.cursor.return_value = cur

    conn_ctx = MagicMock()
    conn_ctx.__enter__ = lambda s: conn
    conn_ctx.__exit__ = MagicMock(return_value=False)
    return conn_ctx, conn, cur


class TestDryRun:
    def test_no_apply_executes_no_truncate(self, monkeypatch, capsys):
        conn_ctx, conn, cur = _make_conn()
        monkeypatch.setattr(reset_mod, "get_connection", lambda: conn_ctx)
        monkeypatch.setattr(sys, "argv", ["reset_verbatims.py"])

        reset_mod.main()

        executed_sql = [str(call.args[0]) for call in cur.execute.call_args_list]
        assert not any("TRUNCATE" in sql.upper() for sql in executed_sql)
        conn.commit.assert_not_called()

    def test_no_apply_prints_the_plan(self, monkeypatch, capsys):
        conn_ctx, conn, cur = _make_conn(db_name="compass_test", n_verbatims=1234)
        monkeypatch.setattr(reset_mod, "get_connection", lambda: conn_ctx)
        monkeypatch.setattr(sys, "argv", ["reset_verbatims.py"])

        reset_mod.main()

        out = capsys.readouterr().out
        assert "DRY-RUN" in out
        assert "1,234" in out or "1234" in out


class TestConfirmation:
    def test_apply_with_wrong_confirm_db_aborts(self, monkeypatch):
        conn_ctx, conn, cur = _make_conn(db_name="compass_prod")
        monkeypatch.setattr(reset_mod, "get_connection", lambda: conn_ctx)
        monkeypatch.setattr(
            sys, "argv",
            ["reset_verbatims.py", "--apply", "--confirm-db", "wrong_name"],
        )

        with patch("scripts.reset_verbatims._run_pg_dump") as mock_dump:
            with pytest.raises(SystemExit):
                reset_mod.main()
            mock_dump.assert_not_called()

        executed_sql = [str(call.args[0]) for call in cur.execute.call_args_list]
        assert not any("TRUNCATE" in sql.upper() for sql in executed_sql)

    def test_cascade_table_categories_mapping_refused(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv",
            [
                "reset_verbatims.py", "--apply", "--confirm-db", "x",
                "--cascade-table", "categories_mapping",
            ],
        )
        with pytest.raises(SystemExit):
            reset_mod.main()


class TestApplyWithCorrectConfirmation:
    def test_truncate_runs_only_after_correct_confirmation(self, monkeypatch, tmp_path):
        conn_ctx, conn, cur = _make_conn(db_name="compass_test", n_verbatims=10)
        monkeypatch.setattr(reset_mod, "get_connection", lambda: conn_ctx)
        monkeypatch.setattr(
            sys, "argv",
            [
                "reset_verbatims.py", "--apply", "--confirm-db", "compass_test",
                "--dump-dir", str(tmp_path),
            ],
        )

        with patch("scripts.reset_verbatims._run_pg_dump") as mock_dump, \
             patch("scripts.reset_verbatims._log_reset", return_value="batch-id") as mock_log:
            reset_mod.main()
            mock_dump.assert_called_once()
            mock_log.assert_called_once()

        executed_sql = [str(call.args[0]) for call in cur.execute.call_args_list]
        assert any(sql.strip().upper() == "TRUNCATE VERBATIMS" for sql in executed_sql)
        # categories_mapping ne doit jamais apparaître dans le TRUNCATE
        assert not any("categories_mapping" in sql for sql in executed_sql)
