import requests
import json
import logging
import pandas as pd
from datetime import datetime, timedelta



# Load configuration file
try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.error("Configuration file 'config.json' not found.")
    raise



# Function to extract data from Polygon.io Stock API
def extract_polygon_data(start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                          end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")):
    """
    Fetch sample stock data from Polygon API.
    """
    try:
        stock_ticker_names = config['polygon']['stock_ticker_names']

        # Convert input strings to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        # Generate range of dates
        date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
        dates = date_list

        # Store the results for all stock tickers and dates
        all_data = {}

        for stock_ticker_name in stock_ticker_names:
            for date in dates:
                polygon_url = f"{config['polygon']['base_url']}/{stock_ticker_name}/{date}?adjusted=true&apiKey={config['polygon'].get('api_key', '')}"

                response = requests.get(polygon_url)
                # If no data is available for the stock ticker on the date (probably sunday or weekend), store 0 values
                if response.status_code == 404:
                    logging.warning(f"No data available for {stock_ticker_name} on {date}.")
                    all_data[stock_ticker_name] = {
                        date: {
                        'symbol': stock_ticker_name,
                        'date': date,
                        'open': 0,
                        'close': 0,
                        'high': 0,
                        'low': 0,
                        'volume': 0
                        }
                    }
                    continue

                response.raise_for_status()  # Raise HTTPError for bad response
                data = response.json()
                logging.info(f"Data for {stock_ticker_name} on {date} successfully retrieved.")

                # Store the data in a dictionary with keys as (ticker_name, date)
                if stock_ticker_name not in all_data:
                    all_data[stock_ticker_name] = {}
                all_data[stock_ticker_name][date] = data

        return all_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Polygon.io: {e}")
        return None


# Function to extract data from Frankfurter Currency API
def extract_frankfurter_data(base_currency='USD',start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                          end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")):
    """
    Fetch currency exchange rate data from Frankfurter API.
    """
    try:
        # Convert input strings to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        # Generate range of dates
        date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
        dates = date_list

        for date in dates:
             # Fetch the latest conversion rate from the API
            frankfurter_url = f"{config['frankfurter']['base_url']}/{date}?from={base_currency}"
            response = requests.get(frankfurter_url)
            response.raise_for_status()
            data = response.json()

            rates = data['rates']
            # Generating the list of dictionaries
            result = [{'currency': currency, 'exchange_rate': rate, 'date': datetime.strptime(date, "%Y-%m-%d").date()} for currency, rate in rates.items()]

            logging.info("Frankfurter exchange rate data successfully retrieved.")
        print(result)
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Frankfurter API: {e}")
        return None

def clean_stock_data(raw_stock_data):
    """
    Cleans and normalizes data from Polygon.io API.
    """
    if not raw_stock_data:
        raise ValueError("Invalid stock data provided.")
    flattened_data = []


    for stock_ticker, dates_data in raw_stock_data.items():

        for date, data in dates_data.items():

            symbol = data.get('symbol', stock_ticker)
            close = data.get('close')
            from_date = data.get('from', date)  # Extract 'from' field, fallback to the current date

            # Append extracted data to the list
            flattened_data.append({
                'stock_ticker': symbol,
                'date': from_date,
                'closing_price': close
            })

    # Convert flattened data into a DataFrame
    stocks_df = pd.DataFrame(flattened_data)

    # Cleanse and format relevant fields
    stocks_df["stock_ticker"] = stocks_df["stock_ticker"].str.upper()
    stocks_df["date"] = pd.to_datetime(stocks_df["date"]).dt.date  # Convert to date only

    return stocks_df


def clean_currency_data(raw_currency_data, base_currency="USD"):
    """
    Cleans and normalizes data from Frankfurter API.
    """
    if not raw_currency_data:
        raise ValueError("Invalid currency data provided.")

    currency_df = pd.DataFrame(raw_currency_data)

    return currency_df


def join_data(stocks_df, currency_df):
    """
    Joins stock data with exchange rates to include currency conversion.
    """
    # Merge stocks with currency rates
    merged_df = currency_df.merge(stocks_df, how="left", left_on="date", right_on="date")
    merged_df["converted_price_in_USD"] = round(merged_df["closing_price"] * merged_df["exchange_rate"],2)  # Example price normalization
    return merged_df

def load_date_to_sql_talbe(merged_df):
    """
    Load the joined data into a SQL table.
    """
    # Create a connection to the database
    connection = sqlite3.connect("stock_data.db")

    # Load the data into the SQL table
    merged_df.to_sql("stock_data_stg", conn, if_exists="replace", index=False)
    sql_file_path = "load_sql.sql"
    # Open and read the SQL file
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()

    # Execute the SQL script
    cursor.executescript(sql_script)

    # Commit changes and close the connection
    connection.commit()
    connection.close()

    print("SQL script executed successfully.")

    # Close the connection

    logging.info("Data successfully loaded into SQL table.")

    return None
# Main function to extract data from both APIs
def main():
    # # Step 1: Initialize Spark Session
    # spark = create_spark_session()

    # Step 2: Fetch data from APIs
    # currency_data = fetch_currency_data(base_currency="USD")  # Get exchange rate data
    # Extract sample stock data from Polygon.io
    logging.info("Extracting data from Polygon.io Stock API...")
    stock_data = extract_polygon_data()
    if stock_data:
        print("Polygon.io Data (Sample):")
        print(json.dumps(stock_data, indent=4))
        stock_df=clean_stock_data(stock_data)

    # Extract currency exchange rates from Frankfurter API
    logging.info("Extracting data from Frankfurter Currency API...")
    currency_data = extract_frankfurter_data()
    if currency_data:
        print("\nFrankfurter Exchange Rate Data:")
        df_currency=clean_currency_data(currency_data)
        stg_table=join_data(stock_df, df_currency)
        print(stg_table)
        # print(currency_data)
        # print(json.dumps(currency_data, indent=4))

        # {
        #     "AAPL": {
        #         "2025-03-12": {
        #             "status": "OK",
        #             "from": "2025-03-12",
        #             "symbol": "AAPL",
        #             "open": 220.14,
        #             "high": 221.75,
        #             "low": 214.91,
        #             "close": 216.98,
        #             "volume": 61482121.0,
        #             "afterHours": 217.41,
        #             "preMarket": 219.8
        #         }
        #     },
        #     "TSLA": {
        #         "2025-03-12": {
        #             "status": "OK",
        #             "from": "2025-03-12",
        #             "symbol": "TSLA",
        #             "open": 247.22,
        #             "high": 251.84,
        #             "low": 241.1,
        #             "close": 248.09,
        #             "volume": 140391349.0,
        #             "afterHours": 253,
        #             "preMarket": 235.07
        #         }
        #     }
        # }

if __name__ == "__main__":
    main()
