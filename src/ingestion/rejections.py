from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class Rejection:
    row_index: int
    row: Mapping[str, Any]
    reason: str


@dataclass
class RejectionReport:
    rejections: list[Rejection] = field(default_factory=list)

    def add(self, row_index: int, row: Mapping[str, Any], reason: str) -> None:
        self.rejections.append(Rejection(row_index=row_index, row=dict(row), reason=reason))

    def extend(self, other: "RejectionReport") -> None:
        self.rejections.extend(other.rejections)

    @property
    def has_rejections(self) -> bool:
        return bool(self.rejections)

    def __iter__(self):
        return iter(self.rejections)

    def __len__(self) -> int:
        return len(self.rejections)
