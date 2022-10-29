from pathlib import Path
from random import random
from typing import Any, Dict, Hashable, Set, Tuple, cast
from uuid import uuid4

from pydb import PyDB
from pydb.db import FindMode

DATA_PATH = Path("data")


def generate_keys(n: int):
    return {str(uuid4()) for _ in range(n)}


def generate_floats(keys: Set[str]) -> Dict[str, float]:
    return {key: random() for key in keys}


def generate_vec3s(keys: Set[str]) -> Dict[str, Tuple[float, float, float]]:
    return {key: (random(), random(), random()) for key in keys}


def destroy_db(db_path: Path):
    db_path.mkdir(exist_ok=True)

    for path in db_path.glob("*"):
        path.unlink()


def generate_db(db: PyDB, n_keys):
    destroy_db(db.data_path)
    keys = generate_keys(n_keys)

    db.dump_field("orientation_down", cast(Dict[Hashable, Any], generate_vec3s(keys)))
    db.dump_field("orientation_right", cast(Dict[Hashable, Any], generate_vec3s(keys)))

    return keys


def _find_all_not_none(*args) -> bool:
    return all(arg is not None for arg in args)


def _find_any_not_none(*args) -> bool:
    return any(arg is not None for arg in args)


def _find_fail(*args) -> bool:
    assert False


class TestPyDB_Small:
    db: PyDB
    keys: Set[str]

    @classmethod
    def setup_class(cls):
        cls.db = PyDB(DATA_PATH)
        cls.keys = generate_db(cls.db, 1_000)

    def test_find_outer(self):
        db = self.db
        keys = self.keys

        found = db.find_keys(
            ["orientation_down", "FAKE_KEY"],
            lambda down, fake: down is not None and fake is None,
            mode=FindMode.OUTER_JOIN,
        )
        assert found == keys

    def test_find_inner(self):
        db = self.db
        keys = self.keys

        found = db.find_keys(
            ["orientation_down"],
            _find_all_not_none,
            mode=FindMode.INNER_JOIN,
        )
        assert found == keys
        found = db.find_keys(
            ["orientation_down", "orientation_right"],
            _find_all_not_none,
            mode=FindMode.INNER_JOIN,
        )
        assert found == keys

        found = db.find_keys(
            ["orientation_down", "FAKE_KEY"],
            _find_fail,
            mode=FindMode.INNER_JOIN,
        )
        assert not found
