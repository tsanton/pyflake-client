# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import collections
from typing import Callable, List, Optional, TypeVar


T = TypeVar("T")


def compare(x, y) -> bool:
    """compare:
    - compare([1,2,3], [1,2,3,3]) -> false
    - compare([1,2,3], [1,2,4]) -> false
    - compare([1,2,3], [1,2,3]) -> true
    - compare([1,2,3], [3,2,1]) -> true
    """
    return collections.Counter(x) == collections.Counter(y)


def find(collection: List[T], predicate: Callable[[T], bool]) -> Optional[T]:
    for item in collection:
        if predicate(item):
            return item
    return None


class Plo:
    @staticmethod
    def find(collection: List[T], predicate: Callable[[T], bool]) -> Optional[T]:
        for item in collection:
            if predicate(item):
                return item
        return None

    @staticmethod
    def contains_by(collection: List[T], predicate: Callable[[T], bool]) -> bool:
        for item in collection:
            if predicate(item):
                return True
        return False
