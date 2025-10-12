import json as json
import os
import sys
from .model import Model
from pathlib import Path
from .solver import Solver

if __name__ == "__main__":
    parent = Path(__file__).parent

    if len(sys.argv) > 1:
        model_file = sys.argv[1]
    else:
        raise ValueError("Model file not specified.")
    model_path = parent / model_file
    model_name = Path(model_file).stem

    solution_base = parent / f"solutions/{model_name}"
    solution_csv = solution_base / "solution.csv"
    solution_csv.parent.mkdir(parents=True, exist_ok=True)

    model = Model.from_json(model_path)
    solver = Solver(model)

    solution = solver.solve()
    solution.write_csv(Path(solution_csv))

    groups_dir = solution_base / "groups"
    teachers_dir = solution_base / "teachers"
    rooms_dir = solution_base / "rooms"
    groups_dir.mkdir(exist_ok=True)
    teachers_dir.mkdir(exist_ok=True)
    rooms_dir.mkdir(exist_ok=True)

    groups = Model.get_available_groups(solution)
    for group in groups:
        timetable = Model.solution_to_timetable(solution, for_group=group)
        dicts = Model.timetable_to_dicts(timetable)

        csv_file = groups_dir / f"timetable_group_{group}.csv"
        json_file = groups_dir / f"timetable_group_{group}.json"

        timetable.write_csv(Path(csv_file))
        json_file.write_text(json.dumps(dicts, indent=2))
    print("[+] Wrote timetables for groups as both CSV and JSON.")

    teachers = Model.get_available_teachers(solution)
    for teacher in teachers:
        timetable = Model.solution_to_timetable(solution, for_teacher=teacher)
        dicts = Model.timetable_to_dicts(timetable)

        teacher_url_safe = teacher.replace(" ", "_").replace(".", "")
        csv_file = teachers_dir / f"timetable_teacher_{teacher_url_safe}.csv"
        json_file = teachers_dir / f"timetable_teacher_{teacher_url_safe}.json"

        timetable.write_csv(Path(csv_file))
        json_file.write_text(json.dumps(dicts, indent=2))
    print("[+] Wrote timetables for teachers as both CSV and JSON.")

    rooms = Model.get_available_rooms(solution)
    for room in rooms:
        timetable = Model.solution_to_timetable(solution, for_room=room)
        dicts = Model.timetable_to_dicts(timetable)

        room_url_safe = room.replace(" ", "_").replace(".", "")
        csv_file = rooms_dir / f"timetable_room_{room_url_safe}.csv"
        json_file = rooms_dir / f"timetable_room_{room_url_safe}.json"

        timetable.write_csv(Path(csv_file))
        json_file.write_text(json.dumps(dicts, indent=2))
    print("[+] Wrote timetables for rooms as both CSV and JSON.")

    summary = Model.get_summary(solution)
    summary_file = solution_base / "summary.json"
    summary_file.write_text(json.dumps(summary, indent=2))
    print("[+] Wrote summary as JSON.")

    conflicts = Model.detect_conflicts(solution)
    conflicts_file = solution_base / "conflicts.json"
    conflicts_file.write_text(json.dumps(conflicts, indent=2))

    total_conflicts = sum(len(conflicts[key]) for key in conflicts)
    if total_conflicts > 0:
        print(f"[!] {total_conflicts} conflicts detected! Check `conflicts.json` for reports.")
    else:
        print("[+] No conflicts detected in the timetable(s).")
