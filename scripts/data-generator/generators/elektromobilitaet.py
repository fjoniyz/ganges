import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep
import os
from typing import Iterable
import uuid


def generate_station_rows() -> Iterable:
    """Returns value in format [id, Timestamp, timeseries_id, Seconds_EnergyConsumption]"""
    # timeseries = station
    while True:
        for station in range(100):
            id = uuid.uuid4()
            date = datetime.now()
            EV_usage = round(uniform(0, uniform(100, 1000)), 2)
            yield [str(id), date, f"{station}" + "EMobility", EV_usage]
        
def get_fields_names():
    return ["id", "Timestamp","timeseries_id", "Seconds_EnergyConsumption"]
    
if __name__ == "__main__":
    """ Writes into a electromobilitydata.csv file with random secondly 
    Endery Consumption we added information about which apartment is affected"""
    
    with open(os.path.dirname(os.path.realpath(__file__)) + '/../data/emobilitydata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        field = get_fields_names()
        writer.writerow(field)
        for row in generate_station_rows():
            writer.writerow(row)
            sleep(0.1)
            
