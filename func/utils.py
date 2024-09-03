from typing import Callable, Optional, TypeVar


T = TypeVar("T")
K = TypeVar("K")


def apply(value: Optional[T], fn: Callable[[T], K]) -> Optional[K]:
    if value is not None:
        return fn(value)
