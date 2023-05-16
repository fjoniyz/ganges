
import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep
# writes into a electromobilitydata.csv file with random secondly Endery Consumption
# we added information about which apartment is affected

with open('emobilitydata.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["Timestamp","timeseries_id", "Seconds_EnergyConsumption"]
    writer.writerow(field)
    
    # timeseries = station
    while True: 
        for station in range(100):
            date = datetime.now()
            EV_usage = round(uniform(0, uniform(100, 1000)), 2)
            writer.writerow([date, f"{station}" + "EMobility", EV_usage])
        sleep(0.1)