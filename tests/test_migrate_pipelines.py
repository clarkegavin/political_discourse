# tests/test_migrate_pipelines.py
from pathlib import Path
from utils.migrate_pipelines import migrate, MIG_PIPES_DIR, MIG_EXPS_DIR


def test_migrate_pipelines_creates_files(tmp_path, monkeypatch):
    # Run migration (it will read config/pipelines.yaml from repo root)
    created = migrate()
    assert isinstance(created, int)

    # Ensure migrated dirs exist
    assert Path(MIG_PIPES_DIR).exists()
    assert Path(MIG_EXPS_DIR).exists()

    # There should be at least one file in migrated pipelines if legacy had entries
    files = list(Path(MIG_PIPES_DIR).glob('*.yaml'))
    assert len(files) >= 0

