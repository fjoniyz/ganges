import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep
import os
from typing import Iterable
import numpy as np
import os

def generate_electricity_rows() -> Iterable:
    """Returns value in format [Datetime, Seconds_EnergyConsumption, apartment_number]"""
    while True:   
        locations = ["street a", "street b", "street c", "street c"]
        locations = int(len(apartments)/len(locations) + 1)*locations
        apartments = np.random.randint(1,6,100) 
        for location, apartment, inhabitants in zip(locations, range(1, 100), apartments):
            date = datetime.now()
            # the number of inhabitants effects energy consumption
            EV = round(uniform(1000*inhabitants, 10000*inhabitants), 2)
            yield [date, f"{apartment}"+"sanierung-apartment", EV, location, inhabitants]
        
def get_fields_names():
    return ["Datetime","Seconds_EnergyConsumption", "apartment_number"]

# What is realistic for a second in energy consumption in a household of 1 to 4 persons
# order of magnitude is completely wrong
# We disregard electricity production (only usage), since it is not common within Rental apartments

# writes into a Mieterstromdata file with random secondly Endery Consumption
if __name__ == "__main__":
    with open(os.path.dirname(os.path.realpath(__file__)) + '/../data/EV_Station_Usage.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        field = get_fields_names()
        writer.writerow(field)

        # apartments is an array with the nuber of inhabitants
        for row in generate_electricity_rows():
            writer.writerow(row)
            sleep(0.1)