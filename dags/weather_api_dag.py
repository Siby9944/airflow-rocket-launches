import requests
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# No API Key needed for weather.gov! 
# We just need a "User-Agent" header to tell them who we are (standard practice).
HEADERS = {'User-Agent': '(my-airflow-study, siby@example.com)'}

def check_frost_warning():
    # Step 1: Get the forecast for Cincinnati area
    # Coordinates for Cincinnati, OH
    url = "https://api.weather.gov/gridpoints/ILN/31,41/forecast"
    
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 200:
        data = response.json()
        # Get the first forecast period (usually 'Tonight' or 'Today')
        current_period = data['properties']['periods'][0]
        
        name = current_period['name']
        temp = current_period['temperature']
        forecast_text = current_period['detailedForecast']
        
        print(f"Forecast for {name}: {temp}°F.")
        print(f"Details: {forecast_text}")
        
        # Agri-Logic: Look for "frost" or "freeze" in the text
        if "frost" in forecast_text.lower() or "freeze" in forecast_text.lower():
            print("!!! AGRI-ALERT: Frost/Freeze expected. Secure the polyhouse! !!!")
        else:
            print("Weather looks safe for crops.")
    else:
        print(f"Failed to reach Weather.gov. Status: {response.status_code}")
        raise Exception("API failure")

with DAG(
    dag_id="agri_weather_v2",
    start_date=pendulum.today('UTC').add(days=-1),
    schedule="@daily",
    catchup=False
) as dag:

    weather_task = PythonOperator(
        task_id="check_cincinnati_frost",
        python_callable=check_frost_warning
    )