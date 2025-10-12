import json as jsonlib
from .model import Model
from pathlib import Path
from .solver import Solver

if __name__ == "__main__":
    parent = Path(__file__).parent
    solution_csv = parent / "solution/solution_one.csv"
    csv = parent / "solution/timetable_one.csv"
    json = parent / "solution/timetable_one.json"
    solution_csv.parent.mkdir(parents=True, exist_ok=True)

    model = Model.from_json(parent / "../examples/one.json")
    solver = Solver(model)

    solution = solver.solve()
    timetable = Model.solution_to_timetable(solution)
    dicts = Model.timetable_to_dicts(timetable)

    solution.write_csv(Path(solution_csv))
    timetable.write_csv(Path(csv))
    _ = json.write_text(jsonlib.dumps(dicts, indent=2))
