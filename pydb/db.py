import os
import pickle
from collections import defaultdict
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Hashable, Iterable, List, NewType, Set, Tuple

from typing_extensions import Protocol

Field = NewType("Field", Dict[Hashable, Any])
FieldRow = NewType("FieldRow", object)


class VarargsPredicate(Protocol):
    def __call__(self, *args: Any) -> bool:
        ...


class FindMode(IntEnum):
    INNER_JOIN = 0
    OUTER_JOIN = 1


class PyDB:
    data_path: Path
    fields: Dict[str, None]

    def __init__(self, data_path: Path, fields: Iterable[str] = None):
        self.data_path = data_path
        self.fields = defaultdict(lambda: None)

        if fields is None:
            fields = tuple()

        for field_name in fields:
            self.fields[field_name] = None

        if not self.data_path.exists():
            os.makedirs(str(self.data_path), exist_ok=True)
        elif not self.data_path.is_dir():
            raise ValueError("data_path is not a directory")

    def _file_for_field(self, field_name: str) -> Path:
        return self.data_path / f"field_{field_name}.pydb"

    def load_field(self, field_name: str) -> Dict[Hashable, Any]:
        field_path = self._file_for_field(field_name)
        if not field_path.exists():
            return defaultdict(lambda: None)

        with field_path.open("rb") as field_file:
            result = pickle.load(field_file)
            return defaultdict(lambda: None, result)

    def dump_field(self, field_name: str, data: Dict[Hashable, Any]):
        field_path = self._file_for_field(field_name)
        with field_path.open("wb") as field_file:
            pickle.dump(data, field_file)

    def find_keys(
        self,
        fields: Tuple[str],
        predicate: VarargsPredicate,
        mode: FindMode = FindMode.INNER_JOIN,
    ) -> Set[Hashable]:
        # NOTE:
        #   This is kind-of temporary as it needs to load each field fully.

        call_param: List[Any]

        field_map: Dict[str, Dict[Hashable, Any]] = dict()

        for field_name in fields:
            field_map[field_name] = self.load_field(field_name)

        result: Set[Hashable] = set()
        if mode == FindMode.INNER_JOIN:
            keys = set(next(iter(field_map.values())).keys())

            for field in field_map.values():
                keys.intersection_update(field.keys())

        elif mode == FindMode.OUTER_JOIN:
            keys = set()
            for field in field_map.values():
                keys.update(field.keys())

        for key in keys:
            # dict() guarantees insertion order, so the order of the
            # parameters is also guaranteed.
            call_param = [field[key] for field in field_map.values()]

            passed = predicate(*call_param)
            if passed:
                result.add(key)

        return result


__all__ = [
    "PyDB",
    "FindMode",
    "Field",
    "FieldRow",
    "VarargsPredicate",
]
