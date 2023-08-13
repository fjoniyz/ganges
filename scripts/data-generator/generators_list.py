from generators import sanierungspotenziale, elektromobilitaet_new, mieterstrom, elektromobilitaet

GENERATORS_LIST = {
    "sanierungsdata": (sanierungspotenziale.get_fields_names, sanierungspotenziale.generate_electricity_rows),
    "mieterstromdata": (mieterstrom.get_fields_names, mieterstrom.generate_electricity_rows),
    "electro-mobility_new": (elektromobilitaet_new.get_fields_names, elektromobilitaet_new.generate_station_rows),
    "electro-mobility": (elektromobilitaet.get_fields_names, elektromobilitaet.generate_station_rows)
}