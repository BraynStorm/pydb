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


def generate_series_uids(keys: Set[str], n_buckets: int) -> Dict[str, str]:
    series_uids = [str(uuid4()) for _ in range(n_buckets)]

    result = dict()
    for i, key in enumerate(keys):
        result[key] = series_uids[i % n_buckets]

    return result


def destroy_db(db_path: Path):
    db_path.mkdir(exist_ok=True)

    for path in db_path.glob("*"):
        path.unlink()


def generate_db(db: PyDB, n_keys):
    destroy_db(db.data_path)
    keys = generate_keys(n_keys)

    db.dump_field("orientation_down", cast(Dict[Hashable, Any], generate_vec3s(keys)))
    db.dump_field("orientation_right", cast(Dict[Hashable, Any], generate_vec3s(keys)))
    db.dump_field(
        "series_uid", cast(Dict[Hashable, Any], generate_series_uids(keys, 10))
    )

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
        found = self.db.find_keys(
            ["orientation_down", "FAKE_KEY"],
            lambda down, fake: down is not None and fake is None,
            mode=FindMode.OUTER_JOIN,
        )
        assert found == self.keys

    def test_find_inner_single_field(self):
        found = self.db.find_keys(
            ["orientation_down"],
            _find_all_not_none,
            mode=FindMode.INNER_JOIN,
        )
        assert found == self.keys

    def test_find_inner_multiple_fields(self):
        found = self.db.find_keys(
            ["orientation_down", "orientation_right"],
            _find_all_not_none,
            mode=FindMode.INNER_JOIN,
        )
        assert found == self.keys

    def test_find_inner_multiple_fields_not_matching(self):
        found = self.db.find_keys(
            ["orientation_down", "FAKE_KEY"],
            _find_fail,
            mode=FindMode.INNER_JOIN,
        )
        assert not found

    def test_group_by_inner(self):
        found = self.db.find_keys_group_by(
            ["orientation_down", "FAKE_KEY"],
            "series_uid",
            _find_fail,
            mode=FindMode.INNER_JOIN,
        )
        assert not found

        found = self.db.find_keys_group_by(
            ["orientation_down"],
            "series_uid",
            _find_any_not_none,
            mode=FindMode.INNER_JOIN,
        )
        assert len(found) == 10

    def test_group_by_outer_fills_empties_with_none(self):
        found = self.db.find_keys_group_by(
            ["orientation_down", "FAKE_KEY"],
            "series_uid",
            lambda o_downs, fakes: all(o_downs) and all(fakes),
            mode=FindMode.OUTER_JOIN,
        )
        assert not found

        found = self.db.find_keys_group_by(
            ["orientation_down", "FAKE_KEY"],
            "series_uid",
            lambda o_downs, fakes: all(o_downs) and not all(fakes),
            mode=FindMode.OUTER_JOIN,
        )
        assert len(found) == 10

    def test_group_by_outer_non_existent_columns(self):
        found = self.db.find_keys_group_by(
            ["FAKE_KEY"],
            "series_uid",
            _find_fail,
            mode=FindMode.OUTER_JOIN,
        )

        assert not found
