import os
import tempfile
import shutil
import yaml
import pytest
from config.config_loader import ConfigLoader


def write_yaml(path: str, data):
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh)


def test_load_file():
    td = tempfile.mkdtemp()
    try:
        p = os.path.join(td, "a.yaml")
        write_yaml(p, {"a": 1, "nested": {"v": "x"}})
        loader = ConfigLoader()
        loaded = loader.load_file(p)
        assert loaded == {"a": 1, "nested": {"v": "x"}}
    finally:
        shutil.rmtree(td)


def test_load_dir():
    td = tempfile.mkdtemp()
    try:
        write_yaml(os.path.join(td, "one.yaml"), {"x": 1})
        write_yaml(os.path.join(td, "two.yaml"), {"y": 2})
        # include a non-yaml to ensure it's skipped
        with open(os.path.join(td, "ignore.txt"), "w") as fh:
            fh.write("skip")
        loader = ConfigLoader()
        d = loader.load_dir(td)
        assert "one" in d and "two" in d
        assert d["one"] == {"x": 1}
        assert d["two"] == {"y": 2}
        assert "ignore" not in d
    finally:
        shutil.rmtree(td)


def test_resolve_refs_simple():
    td = tempfile.mkdtemp()
    try:
        target = os.path.join(td, "target.yaml")
        write_yaml(target, {"val": 123})
        main = os.path.join(td, "main.yaml")
        write_yaml(main, {"nested": {"ref": "target.yaml"}})
        loader = ConfigLoader()
        cfg = loader.load_file(main)
        resolved = loader.resolve_refs(cfg)
        # Confirm ref replaced
        assert resolved["nested"] == {"val": 123}
        # Original cfg not mutated
        assert cfg["nested"] == {"ref": "target.yaml"}
    finally:
        shutil.rmtree(td)


def test_resolve_refs_nested_and_relative():
    td = tempfile.mkdtemp()
    try:
        # c.yaml: final content
        c = os.path.join(td, "c.yaml")
        write_yaml(c, {"final": True})
        # b.yaml: refers to c.yaml
        b = os.path.join(td, "b.yaml")
        write_yaml(b, {"ref": "c.yaml"})
        # a.yaml: refers to b.yaml (nested)
        a = os.path.join(td, "a.yaml")
        write_yaml(a, {"ref": "b.yaml"})
        loader = ConfigLoader()
        cfg = loader.load_file(a)
        resolved = loader.resolve_refs(cfg)
        assert resolved == {"final": True}
        # ensure original unchanged
        assert cfg == {"ref": "b.yaml"}
    finally:
        shutil.rmtree(td)


def test_circular_ref_detected():
    td = tempfile.mkdtemp()
    try:
        a = os.path.join(td, "a.yaml")
        b = os.path.join(td, "b.yaml")
        write_yaml(a, {"ref": "b.yaml"})
        write_yaml(b, {"ref": "a.yaml"})
        loader = ConfigLoader()
        cfg = loader.load_file(a)
        with pytest.raises(ValueError):
            loader.resolve_refs(cfg)
    finally:
        shutil.rmtree(td)

