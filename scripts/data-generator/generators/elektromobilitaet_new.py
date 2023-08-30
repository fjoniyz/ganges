import csv
import random
from time import sleep
import os
from typing import Iterable
import uuid

def generate_station_rows() -> Iterable:
    for station in range(300):
        ae_session_id = str(uuid.uuid4())
        building_type = random.choice(["Residental", 'Commercial', "Healthcare", "Educational"])
        urbanisation_level = random.uniform(0, 1)
        number_loading_stations = random.randint(1, 50)
        number_parking_spaces = random.randint(1, 50)
        start_time_loading = random.randint(1, 720)
        end_time_loading = random.randint(start_time_loading, 1440)
        loading_time = (end_time_loading-start_time_loading)
        kwh = random.uniform(10, 100)
        loading_potential = random.randint(1000, 10000)
        yield [ae_session_id, building_type,
               urbanisation_level, number_loading_stations,
               number_parking_spaces, start_time_loading, end_time_loading, loading_time,
               kwh, loading_potential]

def get_fields_names():
    return ["ae_session_id","building_type", "urbanisation_level",
            "number_loading_stations","number_parking_spaces","start_time_loading","end_time_loading",
            "loading_time", "kwh", "loading_potential"]

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
