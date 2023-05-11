
import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep

# What is realistic for a second in energy consumption in a household of 1 to 4 persons?
# We disregard electricity production (only usage), since it is not common within Rental apartments

# writes into a electromobilitydata.csv file with random secondly Endery Consumption
# we added information about which apartment is affected
with open('emobilitydata.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["Datetime","Seconds_EnergyConsumption", "station"]
    writer.writerow(field)
    
    while True: 
        for station in range(100):
            date = datetime.now()
            EV_usage = round(uniform(0, uniform(100, 1000)), 2)
            writer.writerow([date,EV_usage, station])
        sleep(0.1)