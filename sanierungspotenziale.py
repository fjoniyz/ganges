### TODO: Ziel ist die Nutzung von Kenn- und Erfahrungswerten vergleichbarer Projekte,
# um daraus die optimale Sanierungsstrategie abzuleiten. Neben Projekten, die
# ähnliche Charakteristiken, wie Anzahl der Wohneinheiten und umgesetzte
# Energiemengen, aufweisen, können auch aus der geographischen Lage
# wichtige Anhaltspunkte abgeleitet werden.

# Gebäudedaten / Standort / Netzdaten / Mobilitätsverhalten
import csv 
from datetime import datetime
from random import uniform
from time import sleep
import numpy as np

# there are about 100 apartments
with open('sanierungsdata.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["Datetime","Seconds_EnergyConsumption", "apartment_number", "location"]
    writer.writerow(field)

    # order of magnitude is completely wrong
    # apartments is an array with the nuber of inhabitants
    apartments = np.random.randint(1,6,100) 
    locations = ["street a", "street b", "street c", "street c"]
    while True:   
        for inhabitants, apartment, location in zip(apartments, range(1, 100), locations):
            date = datetime.now()
            # the number of inhabitants effects energy consumption
            EV = round(uniform(1000*inhabitants, 10000*inhabitants), 2)
            writer.writerow([date,EV, apartment, location])
        sleep(0.1)