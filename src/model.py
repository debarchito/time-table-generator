from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import polars as pl


@dataclass
class Model:
    rooms: pl.DataFrame
    teachers: pl.DataFrame
    subjects: pl.DataFrame
    groups: pl.DataFrame
    constraints: dict[str, Any]
    slots: dict[str, pl.DataFrame]

    @classmethod
    def from_json(cls, file: Path) -> "Model":
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        rooms = pl.DataFrame(data["rooms"])
        teachers = pl.DataFrame(data["teachers"])
        subjects = pl.DataFrame(data["subjects"])
        groups = pl.DataFrame(data.get("groups", []))
        constraints = data["constraints"]

        slots = {
            "days": pl.DataFrame({"day": data["slots"]["days"]}),
            "times": pl.DataFrame({"time": data["slots"]["times"]}),
            "breaks": pl.DataFrame(data["slots"].get("breaks", [])),
        }

        return cls(
            rooms,
            teachers,
            subjects,
            groups,
            constraints,
            slots,
        )

    def to_json(self, file: Path) -> None:
        data = {
            "rooms": self.rooms.to_dicts(),
            "slots": {
                "days": self.slots["days"]["day"].to_list(),
                "times": self.slots["times"]["time"].to_list(),
                "breaks": self.slots["breaks"].to_dicts(),
            },
            "teachers": self.teachers.to_dicts(),
            "subjects": self.subjects.to_dicts(),
            "groups": self.groups.to_dicts(),
            "constraints": self.constraints,
        }

        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
