from model import Model
from pathlib import Path
from solver import Solver

if __name__ == "__main__":
    model = Model.from_json(Path("../examples/one.json"))
    solver = Solver(model)
    timetable = solver.solve()
    timetable.write_csv("timetable.csv")
