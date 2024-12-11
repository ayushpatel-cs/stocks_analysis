import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time

# Function to fetch stock data for a single period (5 days at a time)
def fetch_stock_data_chunk(ticker, start_date, end_date, interval="1m"):
    try:
        print(f"Fetching data for {ticker} from {start_date} to {end_date}...")
        data = yf.download(
            tickers=ticker, 
            start=start_date, 
            end=end_date, 
            interval=interval, 
            progress=False
        )
        return data
    except Exception as e:
        print(f"Error fetching {ticker} between {start_date} and {end_date}: {e}")
        return None

# Save data to CSV
def save_to_csv(data, ticker, output_folder="stock_data"):
    os.makedirs(output_folder, exist_ok=True)
    file_path = os.path.join(output_folder, f"{ticker}.csv")
    if os.path.exists(file_path):
        # Append to existing file
        data.to_csv(file_path, mode='a', header=False)
    else:
        # Write new file
        data.to_csv(file_path)
    print(f"Saved data for {ticker} to {file_path}")

# Load S&P 500 tickers
def load_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500 = pd.read_html(url)[0]
    return sp500['Symbol'].tolist()

# Main Function to fetch 3 months of data (90 days) in 5-day increments
def fetch_3_months_data(ticker, interval="1m", output_folder="stock_data"):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Approx 3 months ago
    chunk_duration = timedelta(days=5)

    current_start = start_date
    while current_start < end_date:
        current_end = current_start + chunk_duration
        if current_end > end_date:
            current_end = end_date
        
        # Fetch data chunk
        data_chunk = fetch_stock_data_chunk(ticker, current_start, current_end, interval)
        if data_chunk is not None and not data_chunk.empty:
            save_to_csv(data_chunk, ticker, output_folder)
        else:
            print(f"No data found for {ticker} from {current_start} to {current_end}")

        # Move to the next chunk
        current_start = current_end
        time.sleep(2)  # Sleep to prevent rate-limiting

# Run for all S&P 500 tickers
def main():
    output_folder = "stock_data"
    interval = "1m"  # Minute-level data
    tickers = load_sp500_tickers()
    print(f"Total tickers to fetch: {len(tickers)}")

    for ticker in tickers:
        fetch_3_months_data(ticker, interval, output_folder)
        print(f"Completed fetching data for {ticker}")
    
    print("Data fetching for all tickers completed.")

if __name__ == "__main__":
    main()
