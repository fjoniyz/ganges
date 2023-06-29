from dataclasses import dataclass
from random import sample
from numpy.random import normal
@dataclass
class TaskSimEvCharging:
    min_start: float
    max_start: float
    min_duration: float
    max_duration: float
    min_demand: float
    max_demand: float
    max_power: float

class TaskStorage:
    def __init__(self) -> None:
        pass

class EvChargingPlan:
    def __init__(self, charge: int, start: int, mstart: int, duration: int, mduration: int, demand: int, mdemand: int, power: int) -> None:
        self.charge = charge
        self.start = start
        self.mstart = mstart
        self.duration = duration
        self.mduration = mduration
        self.demand = demand
        self.mdemand = mdemand
        self.power = power

class Normal:
    # TODO: find configuration of Normal Distribution
    def __init__(self, number_one: float, number_two: float) -> None:
        self.distribution = normal(number_one, number_two)
    
    def sample(self, number: int) -> list:
        return sample(self.distribution, number)
        