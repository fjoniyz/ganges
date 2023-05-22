import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep
import numpy as np
import os


# What is realistic for a second in energy consumption in a household of 1 to 4 persons
# order of magnitude is completely wrong
# We disregard electricity production (only usage), since it is not common within Rental apartments

# writes into a Mieterstromdata file with random secondly Endery Consumption
with open(os.path.dirname(os.path.realpath(__file__))  + '/../data/EV_Station_Usage.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["Timestamp","timeseries_id", "Seconds_EnergyConsumption", "location", "inhabitants"]
    writer.writerow(field)

    # order of magnitude is completely wrong
    # apartments is an array with the nuber of inhabitants
    apartments = np.random.randint(1,6,100) 
    locations = ["street a", "street b", "street c", "street c"]
    locations = int(len(apartments)/len(locations) + 1)*locations
    while True:   
        for location, apartment, inhabitants in zip(locations, range(1, 100), apartments):
            date = datetime.now()
            # the number of inhabitants effects energy consumption
            EV = round(uniform(1000*inhabitants, 10000*inhabitants), 2)
            writer.writerow([date, f"{apartment}"+"sanierung-apartment", EV, location, inhabitants])
        sleep(0.1)