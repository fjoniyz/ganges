import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep
import numpy as np


# What is realistic for a second in energy consumption in a household of 1 to 4 persons
# order of magnitude is completely wrong
# We disregard electricity production (only usage), since it is not common within Rental apartments

# writes into a Mieterstromdata file with random secondly Endery Consumption
with open('../data/EV_Station_Usage.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["Datetime","Seconds_EnergyConsumption", "apartment_number"]
    writer.writerow(field)

    
    # apartments is an array with the nuber of inhabitants
    apartments = np.random.randint(1,6,100) 
    while True:   
        for inhabitants, apartment in zip(apartments, range(1, 100)):
            date = datetime.now()
            # the number of inhabitants effects energy consumption
            EV = round(uniform(1000*inhabitants, 10000*inhabitants), 2)
            writer.writerow([date,EV, apartment])
        sleep(0.1)