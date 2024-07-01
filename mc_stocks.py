import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

tickers = ['AAPL', 'NVDA', 'GOOGL', 'MSFT', 'AMZN', 'PYPL', 'TSLA', 'NET', 'JPM',
           'JNJ', 'XOM', 'COST', 'AMD', 'INTC', 'SNOW', 'ORCL', 'CRM', 'COIN', 'PANW', 'DOCN']

end_date = datetime.now()
start_date_1year_ago = end_date - timedelta(days=365)

data = yf.download(tickers, start=start_date_1year_ago, end=end_date)['Close'].round(2)

moving_avg_90 = data.rolling(window=90, min_periods=1).mean().round(2)
moving_avg_180 = data.rolling(window=180, min_periods=1).mean().round(2)
moving_avg_365 = data.rolling(window=365, min_periods=1).mean().round(2)

all_averages = {}
for ticker in tickers:
    all_averages[ticker] = {
        '90-day MA': moving_avg_90[ticker].iloc[-1] if ticker in moving_avg_90.columns else None,
        '180-day MA': moving_avg_180[ticker].iloc[-1] if ticker in moving_avg_180.columns else None,
        '365-day MA': moving_avg_365[ticker].iloc[-1] if ticker in moving_avg_365.columns else None
    }

def fetch_real_time_price(ticker):
    stock = yf.Ticker(ticker)
    todays_data = stock.history(period='1d')
    if not todays_data.empty:
        return round(todays_data['Close'].iloc[-1], 2)
    else:
        return None

def insert_moving_averages():
    conn = None
    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        #print("Connected to Pagila.")

        cur = conn.cursor()

        cur.execute("""
            DROP TABLE IF EXISTS student.mc_stocks;
        """)
        #print("Dropped existing table.")

        cur.execute("""
            CREATE TABLE student.mc_stocks (
                id SERIAL PRIMARY KEY,
                ticker TEXT UNIQUE,
                "Real-time price" NUMERIC(10,2),
                "90-day MA" NUMERIC(10,2),
                "180-day MA" NUMERIC(10,2),
                "365-day MA" NUMERIC(10,2)
            )
        """)
        #print("Created a new table.")

        data_to_insert = []
        for ticker, averages in all_averages.items():
            real_time_price = fetch_real_time_price(ticker)
            data_to_insert.append(
                (ticker,
                 float(real_time_price) if real_time_price is not None else None,
                 float(averages['90-day MA']),
                 float(averages['180-day MA']),
                 float(averages['365-day MA']))
            )

        execute_values(cur, """
            INSERT INTO student.mc_stocks (ticker, "Real-time price", "90-day MA", "180-day MA", "365-day MA")
            VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET
                "Real-time price" = EXCLUDED."Real-time price",
                "90-day MA" = EXCLUDED."90-day MA",
                "180-day MA" = EXCLUDED."180-day MA",
                "365-day MA" = EXCLUDED."365-day MA"
        """, data_to_insert)

        conn.commit()
        #print("Data was inserted successfully.")

    except (psycopg2.OperationalError, psycopg2.ProgrammingError, psycopg2.DatabaseError) as e:
        print(f"Database error: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            #print("Connection closed.")

insert_moving_averages()
