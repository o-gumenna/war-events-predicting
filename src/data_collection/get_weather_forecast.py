import requests

def get_weather_forecast(location: str, api_key: str):

    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    unit_group = "metric"
    content_type = "json"
    
    url = f"{base_url}{location}/next24hours?unitGroup={unit_group}&key={api_key}&contentType={content_type}"
    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        return response.json()     
    except requests.exceptions.RequestException as e:
        print(f"Data request error: {e}")
        return None


# test locally
if __name__ == "__main__":
    TEST_API_KEY = "" 
    TEST_LOCATION = "Kyiv,Ukraine"
    
    print(f"Get forecast for {TEST_LOCATION}...")
    forecast_data = get_weather_forecast(TEST_LOCATION, TEST_API_KEY)
    
    if forecast_data:
        print("Data successfully received!")
        first_hour = forecast_data.get("days", [])[0].get("hours", [])[12]
        print(f"Час: {first_hour.get('datetime')}, Temperature: {first_hour.get('temp')}°C")