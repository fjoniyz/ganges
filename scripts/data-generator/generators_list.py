from generators import sanierungspotenziale, elektromobilitaet, mieterstrom

GENERATORS_LIST = {
    "sanierungsdata": (sanierungspotenziale.get_fields_names, sanierungspotenziale.generate_electricity_rows),
    "mieterstromdata": (mieterstrom.get_fields_names, mieterstrom.generate_electricity_rows),
    "electro-mobility": (elektromobilitaet.get_fields_names ,elektromobilitaet.generate_station_rows)
}