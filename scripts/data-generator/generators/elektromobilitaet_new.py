import csv
from datetime import datetime
import random
from time import sleep
import os
from typing import Iterable
import uuid
from faker import Faker
import datetime

def generate_station_rows() -> Iterable:
    fake = Faker()
    for station in range(100):
        ae_session_id = uuid.uuid4()
        building_type = random.choice(["Residental", 'Commercial', "Healthcare", "Educational"])
        urbanisation_level = random.uniform(0, 1)
        number_loading_stations = random.randint(1, 50)
        number_parking_spaces = random.randint(1, 50)
        start_time_loading = fake.date_time_between(start_date='-1y', end_date='now')
        end_time_loading = fake.date_time_between(start_date=start_time_loading,
                                                  end_date=start_time_loading + datetime.timedelta(days=1))
        loading_time = (end_time_loading-start_time_loading).total_seconds()
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
