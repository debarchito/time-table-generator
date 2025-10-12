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

    @classmethod
    def solution_to_timetable(cls, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.struct(["Subject", "Teacher", "Room", "Groups"])
            .map_elements(
                lambda x: json.dumps(
                    {
                        "subject": x["Subject"],
                        "teacher": x["Teacher"],
                        "room": x["Room"],
                        "group": x["Groups"],
                    },
                    ensure_ascii=False,
                ),
                return_dtype=pl.Utf8,
            )
            .alias("cell_value")
        )

        times = sorted(df["Time"].unique().to_list())
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        days_in_data = [d for d in days if d in df["Day"].unique().to_list()]

        pivot = df.pivot(
            values="cell_value", index="Day", on="Time", aggregate_function="first"
        )

        pivot = (
            pivot.with_columns(
                pl.col("Day")
                .replace({d: i for i, d in enumerate(days_in_data)})
                .alias("day_order")
            )
            .sort("day_order")
            .drop("day_order")
        )

        column_order = ["Day"] + times
        pivot = pivot.select([c for c in column_order if c in pivot.columns])

        return pivot

    @classmethod
    def timetable_to_dicts(
        cls, df: pl.DataFrame
    ) -> list[dict[str, str | dict[str, str]]]:
        result = []

        for row in df.to_dicts():
            parsed_row = {"Day": row["Day"]}
            for key, value in row.items():
                if key != "Day":
                    parsed_row[key] = json.loads(value) if value is not None else None
            result.append(parsed_row)

        return result

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
