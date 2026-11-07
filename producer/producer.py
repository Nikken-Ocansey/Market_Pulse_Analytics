# Defining Dependencies
import json
import os
from pathlib import Path
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Reading configuration from the environment variables in Docker
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "raw_api_data"
)

API_URL = os.getenv(
    "API_URL",
    "https://alpha-vantage.p.rapidapi.com/query"  
)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

POLL_INTERVAL_SECONDS = int(
    os.getenv("POLL_INTERVAL_SECONDS", "15")
)

# Daily request cap, so we dont exhaust daily quota
MAX_REQUESTS_PER_DAY =int(
    os.getenv("MAX_REQUESTS_PER_DAY", "4")
)

STATE_FILE = Path(
    os.getenv("STATE_FILE", "/data/quota.json")
)

# Fail fast if the key is missing, instead of sending headerless
# Requests that just come back 401/403.

if not RAPIDAPI_KEY:
    raise SystemExit("RAPIDAPI_KEY is not set.")

def used_today():
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        state = json.loads(STATE_FILE.read_text())
        return state["count"] if state["date"] == today else 0
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return 0

def record_request():
    today = datetime.now(timezone.utc).date().isoformat()
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"date": today, "count": used_today() + 1}))    

#Creating Kafka producer

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v:json.dumps(v).encode("utf-8")
)

# RapidAPI headers
headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "alpha-vantage.p.rapidapi.com"
}

# Stock symbols
symbols = [
    "MSFT",
    "AMZN"
]

try:
    while True:
        print("Starting new market data Cycle")

        for symbol in symbols:

            # Stop once today's request cap is reached
            if used_today() >= MAX_REQUESTS_PER_DAY:
                print(f"Daily request limit ({MAX_REQUESTS_PER_DAY}) reached. Skipping {symbol}.")
                continue

            print(f"Requesting data for {symbol}.....")

            params = {
                "symbol": symbol,
                "function": "GLOBAL_QUOTE"
            }

            # Count the request before sending, since RapidAPI counts failed calls too
            record_request()
            

            # Request for stock data
            try:

                response = requests.get(
                    API_URL,
                    headers=headers,
                    params=params,
                    timeout=30
                )

                # Checking API response
                print("API status code:", response.status_code)


                if response.status_code == 429:
                     
                    print(f"Rate limit reached for {symbol}. Skipping this symbol.")
                    print(response.text)

                    time.sleep(POLL_INTERVAL_SECONDS)

                    continue


                response.raise_for_status()

                data = response.json()

            except requests.exceptions.Timeout:

                print(
                   f"Request time out for {symbol}. "
                   "Skipping this symbol. "
                )

                continue

            except requests.exceptions.HTTPError as error:

                print(
                   f"HTTP error while requesting {symbol}: {error}"
                )

                continue

            except requests.exceptions.RequestException as error:

                print(
                   f"Request error while requesting {symbol}: {error}"
                )
                continue

            except ValueError:
                print(
                   f"Invalid JSON response received for {symbol}. "
                   "Skipping this symbol"
                )

                continue

            if not data.get("Global Quote"):
                print(f"No quote data for {symbol}. Response was: {data}")
                continue

           
            # Create Kafka message

            message = {
               "symbol": symbol,
               "data": data,
               "ingestion_time": datetime.now(timezone.utc).isoformat()
           }

            # Sending message to Kafka
            try:

                producer.send(
                    KAFKA_TOPIC,
                    value=message
                ).get(timeout=10)
            
                print(f"Successfully sent {symbol} data to Kafka topic '{KAFKA_TOPIC}'")

            except KafkaError as error:

                print(
                   f"Kafka error while sending {symbol}: {error}"
                )

                continue

            # Wait before requesting for the next stock
            print(
               f"Waiting {POLL_INTERVAL_SECONDS} seconds before the next request..."
            )

            time.sleep(POLL_INTERVAL_SECONDS)
        

        # Make sure messages are delivered
        try:
           
            producer.flush()

            print("Kafka messages flushed successfully.")
        except KafkaError as error:

            print(f"Kafka flush error: {error}")

        print("Cycle complete")

        if used_today() >= MAX_REQUESTS_PER_DAY:
            print("Daily quota used up. Sleeping 1 hour before checking again.")
            time.sleep(3600)

except KeyboardInterrupt:

    print("Producer stopped by user.")

finally:

    print("Closing Kafka producer...")

    producer.close()

    print("Kafka producer closed.")
