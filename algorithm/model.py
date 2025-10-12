import json
import polars as pl
from typing import Any
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


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

        rooms_data = data["rooms"]
        for room in rooms_data:
            if "capacity" not in room:
                room["capacity"] = 50

        groups_data = data.get("groups", [])
        for group in groups_data:
            if "size" not in group:
                group["size"] = 0

        rooms = pl.DataFrame(rooms_data)
        teachers = pl.DataFrame(data["teachers"])
        subjects = pl.DataFrame(data["subjects"])
        groups = pl.DataFrame(groups_data)
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
    def get_available_groups(cls, df: pl.DataFrame) -> list[str]:
        return sorted(df.select("Groups").unique().to_series().to_list())

    @classmethod
    def get_available_teachers(cls, df: pl.DataFrame) -> list[str]:
        return sorted(df.select("Teacher").unique().to_series().to_list())

    @classmethod
    def get_available_rooms(cls, df: pl.DataFrame) -> list[str]:
        return sorted(df.select("Room").unique().to_series().to_list())

    @classmethod
    def get_available_times(cls, df: pl.DataFrame) -> list[str]:
        return sorted(df.select("Time").unique().to_series().to_list())

    @classmethod
    def get_available_days(cls, df: pl.DataFrame) -> list[str]:
        days_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        available_days = df.select("Day").unique().to_series().to_list()
        return [day for day in days_order if day in available_days]

    @classmethod
    def solution_to_timetable(
        cls,
        df: pl.DataFrame,
        for_group: Optional[str] = None,
        for_teacher: Optional[str] = None,
        for_room: Optional[str] = None,
    ) -> pl.DataFrame:
        filter_count = sum(x is not None for x in [for_group, for_teacher, for_room])
        if filter_count > 1:
            raise ValueError(
                "Cannot specify more than one filter: for_group, for_teacher, or for_room"
            )
        elif filter_count == 0:
            raise ValueError(
                "Must specify exactly one filter: for_group, for_teacher, or for_room"
            )

        if for_group is not None:
            df = df.filter(pl.col("Groups") == for_group)
        elif for_teacher is not None:
            df = df.filter(pl.col("Teacher") == for_teacher)
        elif for_room is not None:
            df = df.filter(pl.col("Room") == for_room)

        if df.height == 0:
            default_times = ["08:00", "09:00", "11:00", "13:00", "14:00", "16:00"]
            default_days = ["Mon", "Tue", "Wed", "Thu", "Fri"]
            empty_df = pl.DataFrame({"Day": default_days})
            for time in default_times:
                empty_df = empty_df.with_columns(pl.lit(None).alias(time))
            return empty_df

        if for_room is not None:
            df = df.with_columns(
                pl.struct(["Subject", "Teacher", "Groups"])
                .map_elements(
                    lambda x: json.dumps(
                        {
                            "subject": x["Subject"],
                            "teacher": x["Teacher"],
                            "group": x["Groups"],
                            "room": for_room,
                        },
                        ensure_ascii=False,
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("cell_value")
            )
        else:
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

        times = cls.get_available_times(df)
        days_in_data = cls.get_available_days(df)

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

    @classmethod
    def get_summary(cls, df: pl.DataFrame, rooms_df: pl.DataFrame = None, groups_df: pl.DataFrame = None) -> dict[str, Any]:
        summary = {
            "total_classes": df.height,
            "groups": cls.get_available_groups(df),
            "teachers": cls.get_available_teachers(df),
            "rooms": cls.get_available_rooms(df),
            "days": cls.get_available_days(df),
            "times": cls.get_available_times(df),
            "subjects": sorted(df.select("Subject").unique().to_series().to_list()),
        }

        summary["stats"] = {
            "classes_per_group": df.group_by("Groups").len().sort("Groups").to_dicts(),
            "classes_per_teacher": df.group_by("Teacher")
            .len()
            .sort("Teacher")
            .to_dicts(),
            "classes_per_room": df.group_by("Room").len().sort("Room").to_dicts(),
            "classes_per_day": df.group_by("Day").len().sort("Day").to_dicts(),
        }

        if rooms_df is not None:
            summary["room_capacities"] = {
                room["id"]: room.get("capacity", 50)
                for room in rooms_df.to_dicts()
            }

        if groups_df is not None:
            summary["group_sizes"] = {
                group["id"]: group.get("size", 0)
                for group in groups_df.to_dicts()
            }

        return summary

    @classmethod
    def detect_conflicts(cls, df: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
        conflicts = {
            "teacher_conflicts": [],
            "room_conflicts": [],
            "group_conflicts": [],
        }

        time_slots = df.select(["Day", "Time"]).unique()

        for row in time_slots.to_dicts():
            day, time = row["Day"], row["Time"]
            slot_classes = df.filter((pl.col("Day") == day) & (pl.col("Time") == time))

            teacher_counts = slot_classes.group_by("Teacher").len()
            room_counts = slot_classes.group_by("Room").len()
            group_counts = slot_classes.group_by("Groups").len()

            for teacher_row in teacher_counts.filter(pl.col("len") > 1).to_dicts():
                teacher_classes = slot_classes.filter(
                    pl.col("Teacher") == teacher_row["Teacher"]
                ).to_dicts()
                conflicts["teacher_conflicts"].append(
                    {
                        "day": day,
                        "time": time,
                        "teacher": teacher_row["Teacher"],
                        "classes": teacher_classes,
                    }
                )

            for room_row in room_counts.filter(pl.col("len") > 1).to_dicts():
                room_classes = slot_classes.filter(
                    pl.col("Room") == room_row["Room"]
                ).to_dicts()
                conflicts["room_conflicts"].append(
                    {
                        "day": day,
                        "time": time,
                        "room": room_row["Room"],
                        "classes": room_classes,
                    }
                )

            for group_row in group_counts.filter(pl.col("len") > 1).to_dicts():
                group_classes = slot_classes.filter(
                    pl.col("Groups") == group_row["Groups"]
                ).to_dicts()
                conflicts["group_conflicts"].append(
                    {
                        "day": day,
                        "time": time,
                        "group": group_row["Groups"],
                        "classes": group_classes,
                    }
                )

        return conflicts

    @classmethod
    def detect_capacity_violations(cls, df: pl.DataFrame, rooms_df: pl.DataFrame, groups_df: pl.DataFrame) -> list[dict[str, Any]]:
        """
        Detect cases where group size exceeds room capacity
        """
        violations = []

        room_capacity = {room["id"]: room.get("capacity", 50) for room in rooms_df.to_dicts()}
        group_size = {group["id"]: group.get("size", 0) for group in groups_df.to_dicts()}

        for row in df.to_dicts():
            room_id = row["Room"]
            groups_str = row["Groups"]

            group_ids = [g.strip() for g in groups_str.split(",")]

            total_students = sum(group_size.get(group_id, 0) for group_id in group_ids)
            room_cap = room_capacity.get(room_id, 50)

            if total_students > room_cap:
                violations.append({
                    "day": row["Day"],
                    "time": row["Time"],
                    "subject": row["Subject"],
                    "teacher": row["Teacher"],
                    "room": room_id,
                    "room_capacity": room_cap,
                    "groups": group_ids,
                    "total_students": total_students,
                    "overflow": total_students - room_cap
                })

        return violations
