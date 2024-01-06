import random

def generate_temperature_data(file_name, num_rows):
    places = ["Hamburg", "Bulawayo", "Palembang", "St. John's", "Cracow",
              "Bridgetown", "Istanbul", "Roseau", "Conakry"]
    temperature_range = {'min': -30, 'max': 50}  # Example temperature range between -30 and 50 degrees Celsius

    with open(file_name, mode='w', newline='') as file:
        for _ in range(num_rows):
            place = random.choice(places)
            temperature = round(random.uniform(temperature_range['min'], temperature_range['max']), 1)
            file.write(f"{place};{temperature}\n")

# Be very careful with setting num_rows to a high value as it can create a huge file
generate_temperature_data('temperature_records.txt', 1000000000)