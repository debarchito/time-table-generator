from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any
import polars as pl
import json


@dataclass
class Model:
    rooms: pl.DataFrame
    teachers: pl.DataFrame
    subjects: pl.DataFrame
    constraints: Dict[str, Any]
    timeslots: Dict[str, pl.DataFrame]

    @classmethod
    def from_json(cls, file: Path) -> "Model":
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        rooms = pl.DataFrame(data["rooms"])
        teachers = pl.DataFrame(data["teachers"])
        subjects = pl.DataFrame(data["subjects"])
        constraints = data["constraints"]

        timeslots = {
            "days": pl.DataFrame(data["timeslots"]["days"]),
            "slots_per_day": pl.DataFrame(data["timeslots"]["slots_per_day"]),
            "breaks": pl.DataFrame(data["timeslots"].get("breaks", [])),
        }

        return cls(
            rooms=rooms,
            teachers=teachers,
            subjects=subjects,
            constraints=constraints,
            timeslots=timeslots,
        )

    def to_json(self, file: Path) -> None:
        data = {
            "rooms": self.rooms.to_dicts(),
            "timeslots": {
                "days": self.timeslots["days"].to_series().to_list(),
                "slots_per_day": self.timeslots["slots_per_day"].to_series().to_list(),
                "breaks": self.timeslots["breaks"].to_dicts(),
            },
            "teachers": self.teachers.to_dicts(),
            "subjects": self.subjects.to_dicts(),
            "constraints": self.constraints,
        }

        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
