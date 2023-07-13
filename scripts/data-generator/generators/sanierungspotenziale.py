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
import os
import uuid

def generate_electricity_rows():
    """Returns value in format [id, Datetime, Seconds_EnergyConsumption, apartement_number, location]"""
    # there are about 100 apartments
    # order of magnitude is completely wrong
    # apartments is an array with the number of inhabitants
    while True:
        id = uuid.uuid4()
        apartments = np.random.randint(1,6,100) 
        locations = ["street a", "street b", "street c", "street c"]
        locations = int(len(apartments)/len(locations) + 1)*locations
        development_status = ["Old Construction", "New Construction"]
        development_status = int(len(apartments)/len(development_status) + 1)*development_status
        for development, location, apartment, inhabitants in zip(development_status, locations, range(1, 100), apartments):
            date = datetime.now()
            # the number of inhabitants effects energy consumption
            EV = round(uniform(1000*inhabitants, 10000*inhabitants), 2)
            yield [id, date, f"{apartment}"+"sanierung-apartment", EV, location, inhabitants, development]
        
def get_fields_names():
    return ["id", "Datetime", "Seconds_EnergyConsumption", "apartment_number", "location"]

if __name__ == "__main__":
    with open(os.path.dirname(os.path.realpath(__file__)) + '/../data/sanierungsdata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        field = get_fields_names()
        writer.writerow(field)

        for row in generate_electricity_rows():
            writer.writerow(row)
            sleep(0.1)
