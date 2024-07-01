## Stock Data Analysis and Storage with PostgreSQL

Python script for automated stock analysis.  
Calculates 90, 180, and 365-day moving averages and updates a PostgreSQL database with real-time prices.

**Key Features**  

* **Stock Data Retrieval:** Uses the `yfinance` library to download historical closing prices from Yahoo Finance for a specified list of tickers. 
* **Moving Average Calculation:** Calculates 90-day, 180-day, and 365-day moving averages to identify trends in stock prices.
* **Real-Time Price Updates:** Fetches the most recent closing prices from Yahoo Finance.
* **PostgreSQL Integration:** Stores both the calculated moving averages and real-time prices in a PostgreSQL database for easy access and analysis.
* **Data Integrity:** Handles potential conflicts in the database by updating existing records instead of creating duplicates.
* **Error Handling:** Includes error handling mechanisms to catch and report database connection or query issues.

