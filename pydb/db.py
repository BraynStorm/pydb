import os
import pickle
from collections import defaultdict
from enum import IntEnum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    List,
    NewType,
    Set,
    Tuple,
    cast,
)

from typing_extensions import Protocol

Field = NewType("Field", Dict[Hashable, Any])
FieldRow = NewType("FieldRow", object)


class VarargsAnyPredicate(Protocol):
    def __call__(self, *args: Any) -> bool:
        ...


class VarargsListsPredicate(Protocol):
    def __call__(self, *args: List[Any]) -> bool:
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

    def find_keys_group_by(
        self,
        fields: Tuple[str, ...],
        group_by_field: str,
        predicate: VarargsListsPredicate,
        mode: FindMode = FindMode.INNER_JOIN,
    ) -> Dict[Hashable, List[Hashable]]:
        group_field: Dict[Hashable, Any]
        field_keys: Set[Hashable]
        call_params: List[List[Hashable]]
        result: Dict[Hashable, List[Hashable]]
        grouped: Dict[Hashable, List[Hashable]]
        field_map: Dict[str, Dict[Hashable, Any]]

        field_map = dict()
        for field_name in fields:
            field_map[field_name] = self.load_field(field_name)

        if group_by_field in field_map:
            group_field = field_map[group_by_field]
        else:
            group_field = self.load_field(group_by_field)

        # Determine keys to use.
        if mode == FindMode.INNER_JOIN:
            field_keys = set(next(iter(field_map.values())).keys())
            for field in field_map.values():
                field_keys.intersection_update(field.keys())
        elif mode == FindMode.OUTER_JOIN:
            field_keys = set()
            for field in field_map.values():
                field_keys.update(field.keys())

        grouped = defaultdict(list)
        for key in field_keys:
            group_key = cast(Hashable, group_field[key])
            grouped[group_key].append(key)

        result = defaultdict(list)
        for group_key, group_field_keys in grouped.items():
            call_params = [[]] * len(fields)
            for i, field_name in enumerate(fields):
                field = field_map[field_name]
                call_params[i] = [field[key] for key in group_field_keys]

            passed = predicate(*call_params)
            if passed:
                result[group_key] = group_field_keys

        return result

    def find_keys(
        self,
        fields: Tuple[str],
        predicate: VarargsAnyPredicate,
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
    "VarargsAnyPredicate",
    "VarargsListsPredicate",
]
