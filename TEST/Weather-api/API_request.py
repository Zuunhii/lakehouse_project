import requests
api_key = "c44861ca77de3d13ff3d148323a81de1"
api_url = f"https://api.weatherstack.com/current?access_key={api_key}&query=New York"


def fetch_data():
    print("Fetching data from Weatherstack API...")
    try:
        respone = requests.get(api_url)
        respone.raise_for_status()
        print("Data fetched successfully.")
        return respone.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise
        

def mock_fetch_data():
    
    return {
        "request": {
            "type": "City",
            "query": "New York, United States of America",
            "language": "en",
            "unit": "m"
        },
        "location": {
            "name": "New York",
            "country": "United States of America",
            "region": "New York",
            "lat": "40.714",
            "lon": "-74.006",
            "timezone_id": "America/New_York",
            "localtime": "2024-06-15 10:00",
            "localtime_epoch": 1718361600,
            "utc_offset": "-4.0"
        },
        "current": {
            "observation_time": "02:00 PM",
            "temperature": 22,
            "weather_code": 113,
            "weather_icons": [
                "https://assets.weatherstack.com/images/wsymbols01_png_64/wsymbol_0001_sunny.png"
            ],
            "weather_descriptions": [
                "Sunny"
            ],
            "wind_speed": 13,
            "wind_degree": 240,
            "wind_dir": "WSW",
            "pressure": 1015,
            "precip": 0,
            "humidity": 60,
            "cloudcover": 0,
            "feelslike": 24,
            "uv_index": 7,
            "visibility": 16
        }
    }
