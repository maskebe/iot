import base64
import json
from google.cloud import bigquery
from twilio.rest import Client


def predict(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    registry_id = event['attributes']['deviceRegistryId']
    my_id = None
    if registry_id.startswith("registre-"):
        my_id = registry_id.split("registre-")[1]
    else:
        print(f"Error! Invalid registry ID format : '{registry_id}'")

    # Get canal obstruction info
    message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Receiving sensor data (canal obstruction) : {message}")
    data = json.loads(message)
    canal_obstruction = data["obstruction"]
    print(f"Canal obstruction : {canal_obstruction}")

    if int(canal_obstruction) == 0:
        return

    # Prediction based on the last 12 hours recorded sensors data (temperature, humidity, month)
    bq_client = bigquery.Client()
    predict_query = f"""
         SELECT
           *
         FROM
           ML.PREDICT(MODEL `ml_model_meteo.rainfall_prediction_{my_id}`, (
         SELECT
           CAST(humidity as INT64) AS humidity,
           CAST(temperature as FLOAT64) AS temperature,
           EXTRACT(MONTH FROM timestamp) AS month
         FROM
            `meteorological_station_dataset.station_{my_id}`
         WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) AND CURRENT_TIMESTAMP() 
         ))
         ORDER BY predicted_label DESC
         LIMIT 1
        """
    query_job = bq_client.query(predict_query)

    will_rain = False
    for row in query_job:
        # Row((1, [{'label': 1, 'prob': 0.5441665957315596}, {'label': 0, 'prob': 0.45583340426844043}], 87, 23.9, 4), {'predicted_label': 0, 'predicted_label_probs': 1, 'humidity': 2, 'temperature': 3, 'month': 4})
        if row[0] == 1:
            will_rain = True
    print(f"Rain prediction : {will_rain}")

    # Get last canal water level
    walter_level_query = f"""
        SELECT 
          *
        FROM 
          `meteorological_station_dataset.station_{my_id}`
        WHERE 
          timestamp = (SELECT MAX(timestamp) FROM `meteorological_station_dataset.station_{my_id}`)
        ORDER BY timestamp;
       """
    query_job = bq_client.query(walter_level_query)

    water_level = 0
    for row in query_job:
        water_level = row["water_level"]
    # FULL = 2, HALF_FULL = 1 and EMPTY = 0
    EMPTY, HALF_FULL, FULL = 0, 1, 2
    print(f"Canal water level : {water_level}")

    # Use rule engine to predict flood risk (NULL = 0, LOW = 1, M0DERATE = 2, HIGH = 3)
    NULL, LOW, MODERATE, HIGH = 0, 1, 2, 3
    flood_risk = NULL
    
    # COMPETER LA SUITE... 

















