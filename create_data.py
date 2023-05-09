import csv 
from datetime import datetime
from random import uniform, randint
from time import sleep

# writes into a Mieterstromdata file with random AEP_hourly data
with open('mieterstromdata.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    ## TODO: What is the schema of data for Mieterstromkennzahlen?
    # As a replacements it generates random data from AEP_hourly.csv
    field = ["Datetime","AEP_MW"]
    writer.writerow(field)
    
    for x in range(randint(10, 1000)): 
        date = datetime.now()
        AEP_MV = round(uniform(1000, 10000), 4)
        writer.writerow([date,AEP_MV])
        sleep(0.1)