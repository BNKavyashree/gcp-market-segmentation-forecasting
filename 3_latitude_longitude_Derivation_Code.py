import googlemaps
import pandas as pd

gmaps = googlemaps.Client(key='GOOGLE_MAPS_API_KEY')

cities = [
    "Denver, US",
    "New York City, US",
    "Philadelphia, US",
    "Shanghai, CH",
    "Sao Paulo, BR",
    "Chicago, US",
    "Beijing, CH",
    "Detroit, US",
    "Rio de Janeiro, BR",
    "Wuhan, CH",
    "Washington DC, US",
    "Belo Horizonte, BR",
    "Frankfurt, DE",
    "Hamburg, DE",
    "München, DE",
    "Berlin, DE",
    "Hannover, DE",
    "Leipzig, DE",
    "Marseille, FR",
    "Heidelberg, DE",
    "Brest, FR",
    "Paris, FR",
    "Magdeburg, DE"
]

results = []

for city in cities:
    geocode_result = gmaps.geocode(city)
    if geocode_result:
        location = geocode_result[0]['geometry']['location']
        results.append({
            'City': city.split(',')[0],
            'Country': city.split(',')[1].strip(),
            'Latitude': location['lat'],
            'Longitude': location['lng']
        })


df = pd.DataFrame(results)
print(df)

df.to_csv('MPD_CustomersAttr_location_Coordinates.csv', index=False)
