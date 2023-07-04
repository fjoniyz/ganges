from dataclasses import dataclass
from random import sample
from numpy.random import normal
from pydantic import BaseModel
from typing import Literal

class TaskSimEvCharging(BaseModel):
    """
    Configuration for generating EV events.
    """

    type: Literal["TaskSimEvCharging"] = "TaskSimEvCharging"

    """type definition of class for fail save parsing"""

    min_start: int = 300

    """[minute] beginning minute of day for possible start"""

    max_start: int = 1320

    """minute] end minute of day of possible start"""

    min_duration: int = 15

    """[minutes] minimal charging duration"""

    max_duration: int = 360

    """[minutes] maximal charging duration"""

    min_demand: int = 10

    """[kWh] minimal charging demand"""

    max_demand: int = 60

    """[kWh] maximal charging demand"""

    power: list[float] = [11.0, 22.0]

    """[kW] list of power level for random choice."""

    # export_ts: dict[Literal["pipo", "demand", "power"], DepTsMath]

    """mapping of dependency time series for values PlugIn PlugOff 'pipo', 'demand' and

    'power'"""

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
