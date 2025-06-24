import requests, json, pandas as pd

API_KEY = '2e8bac161af534d61247a4c4e703c706'
cities = ['Colombo', 'Kandy', 'Galle']

records = []
for city in cities:
    res = requests.get(
        f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    )
    data = res.json()
    records.append({
        "city": city,
        "temp_C": round(data["main"]["temp"] - 273.15, 2),
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"]
    })

# Save
df = pd.DataFrame(records)
df.to_csv("weather_data.csv", index=False)
with open("weather_data.json", "w") as f:
    json.dump(records, f, indent=2)
