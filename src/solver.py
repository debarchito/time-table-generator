from pathlib import Path
from typing import Dict, Any, List, Tuple
import polars as pl
from model import Model


def generate_events(model: Model) -> pl.DataFrame:
    subjects = model.subjects.to_dicts()
    teachers = model.teachers.to_dicts()
    subj_to_teachers: Dict[str, List[str]] = {}
    for t in teachers:
        for s in t.get("subjects", []):
            subj_to_teachers.setdefault(s, []).append(t["id"])
    rows = []
    for subj in subjects:
        sid = subj["id"]
        n = int(subj.get("classes_per_week", 1))
        for i in range(1, n + 1):
            rows.append(
                {
                    "event_id": f"{sid}__{i}",
                    "subject": sid,
                    "credits": subj.get("credits"),
                    "size": subj.get("size", None),
                    "candidate_teachers": subj_to_teachers.get(sid, []),
                }
            )
    return pl.DataFrame(rows)


def greedy_schedule(
    model: Model, events: pl.DataFrame
) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
    days = model.timeslots["days"].to_series().to_list()
    slots = model.timeslots["slots_per_day"].to_series().to_list()
    breaks_df = model.timeslots["breaks"]
    if "day" in breaks_df.columns and "time" in breaks_df.columns:
        breaks = {(r["day"], r["time"]) for r in breaks_df.to_dicts()}
    else:
        breaks = set()
    expanded_slots = [(d, s) for d in days for s in slots if (d, s) not in breaks]
    rooms_list = sorted(model.rooms.to_dicts(), key=lambda r: r.get("capacity", 0))
    teacher_busy: Dict[str, set] = {t["id"]: set() for t in model.teachers.to_dicts()}
    room_busy: Dict[str, set] = {r["id"]: set() for r in model.rooms.to_dicts()}
    assignments = []
    if "size" in events.columns:
        events_sorted = events.sort("size", descending=True).to_dicts()
    else:
        events_sorted = events.to_dicts()
    for ev in events_sorted:
        assigned = False
        subject = ev["subject"]
        candidates = ev.get("candidate_teachers", []) or [None]
        for teacher in candidates:
            for day, time in expanded_slots:
                if teacher is not None and (day, time) in teacher_busy.get(
                    teacher, set()
                ):
                    continue
                for room in rooms_list:
                    room_id = room["id"]
                    cap = room.get("capacity", 0)
                    size = ev.get("size") or 0
                    if cap < size:
                        continue
                    if (day, time) in room_busy.get(room_id, set()):
                        continue
                    assignments.append(
                        {
                            "event_id": ev["event_id"],
                            "subject": subject,
                            "teacher": teacher,
                            "room": room_id,
                            "day": day,
                            "time": time,
                        }
                    )
                    if teacher is not None:
                        teacher_busy.setdefault(teacher, set()).add((day, time))
                    room_busy.setdefault(room_id, set()).add((day, time))
                    assigned = True
                    break
                if assigned:
                    break
            if assigned:
                break
        if not assigned:
            assignments.append(
                {
                    "event_id": ev["event_id"],
                    "subject": subject,
                    "teacher": None,
                    "room": None,
                    "day": None,
                    "time": None,
                }
            )
    assignments_df = pl.DataFrame(assignments)
    unassigned = [r for r in assignments if r["room"] is None]
    return assignments_df, unassigned


if __name__ == "__main__":
    model = Model.from_json(Path("../examples/one.json"))
    events = generate_events(model)
    assignments_df, unassigned = greedy_schedule(model, events)
    print(assignments_df)
    assignments_df.write_csv("assignments.csv")
    print(unassigned)
