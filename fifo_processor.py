import pandas as pd
import numpy as np
from datetime import datetime

def process_trades_fifo(csv_file):
    """
    Process trade data using FIFO method to determine remaining purchases,
    preserving the original order of trades in the input file.
    
    Args:
        csv_file: Path to the CSV file containing trade data with columns:
                 ClientCode, TradeDate, Segment, ScripName, BuyQty, BuyPrice,
                 BuyAmount, SellQty, SellPrice, SellAmount, OrderNo
    
    Returns:
        DataFrame with remaining purchases after all sales are matched
    """
    # Read the CSV file
    print(f"Reading trade data from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # Data cleaning and preparation
    print("Cleaning and preparing data...")
    
    # Convert date column to datetime with explicit dd/mm/yyyy format
    df['TradeDate'] = pd.to_datetime(df['TradeDate'], format='%d/%m/%Y', errors='coerce')
    
    # Ensure numeric columns are properly typed
    df['BuyQty'] = pd.to_numeric(df['BuyQty'], errors='coerce').fillna(0)
    df['SellQty'] = pd.to_numeric(df['SellQty'], errors='coerce').fillna(0)
    
    # Clean price columns - remove any non-numeric characters and convert
    for col in ['BuyPrice', 'SellPrice']:
        if col in df.columns:
            if df[col].dtype == object:  # If string type
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('₹', '')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Check if data might not be chronologically sorted - better method that accounts for same dates
    is_sorted = True
    for company in df['ScripName'].unique():
        company_df = df[df['ScripName'] == company]
        # Check if dates are monotonically increasing (allows equal dates)
        if not company_df['TradeDate'].is_monotonic_increasing:
            is_sorted = False
            print(f"Warning: Trades for {company} are not in chronological order.")
    
    if not is_sorted:
        print("\nWARNING: Some trades appear to be out of chronological order.")
        print("The FIFO method assumes trades are processed in chronological order.")
        print("Make sure this is intended, or consider sorting by date before processing.\n")
    
    # Initialize a list to store remaining purchases
    remaining_purchases = []
    
    # Process by company (ScripName)
    companies = df['ScripName'].unique()
    print(f"Found {len(companies)} unique companies in the data")
    
    for company in companies:
        print(f"\nProcessing trades for: {company}")
        company_trades = df[df['ScripName'] == company]
        
        # Initialize queue for purchases
        purchase_queue = []
        
        for _, trade in company_trades.iterrows():
            trade_date = trade['TradeDate']
            
            # Process buy transactions
            if trade['BuyQty'] > 0:
                buy_qty = trade['BuyQty']
                buy_price = trade['BuyPrice']
                
                purchase_queue.append({
                    'TradeDate': trade_date,
                    'ScripName': company,
                    'Segment': trade['Segment'],
                    'ClientCode': trade['Client Code'],
                    'BuyQty': buy_qty,
                    'BuyPrice': buy_price,
                    'OrderNo': trade['OrderNo'] if 'OrderNo' in trade and not pd.isna(trade['OrderNo']) else '',
                    'RemainingQty': buy_qty,
                    'Original_Index': _  # Keep track of original row index
                })
                print(f"  Buy: {buy_qty} shares at {buy_price} on {trade_date.strftime('%d/%m/%Y') if not pd.isna(trade_date) else 'Unknown date'}")
            
            # Process sell transactions using FIFO
            if trade['SellQty'] > 0:
                sell_qty = trade['SellQty']
                sell_price = trade['SellPrice'] if 'SellPrice' in trade and not pd.isna(trade['SellPrice']) else 0
                
                print(f"  Sell: {sell_qty} shares at {sell_price} on {trade_date.strftime('%d/%m/%Y') if not pd.isna(trade_date) else 'Unknown date'}")
                
                # Match sales with purchases using FIFO
                qty_to_sell = sell_qty
                i = 0
                
                while qty_to_sell > 0 and i < len(purchase_queue):
                    if purchase_queue[i]['RemainingQty'] <= qty_to_sell:
                        # Use all remaining quantity from this purchase
                        qty_used = purchase_queue[i]['RemainingQty']
                        qty_to_sell -= qty_used
                        purchase_queue[i]['RemainingQty'] = 0
                        print(f"    Matched: {qty_used} shares from purchase on {purchase_queue[i]['TradeDate'].strftime('%d/%m/%Y') if not pd.isna(purchase_queue[i]['TradeDate']) else 'Unknown date'}")
                    else:
                        # Use partial quantity from this purchase
                        purchase_queue[i]['RemainingQty'] -= qty_to_sell
                        print(f"    Matched: {qty_to_sell} shares from purchase on {purchase_queue[i]['TradeDate'].strftime('%d/%m/%Y') if not pd.isna(purchase_queue[i]['TradeDate']) else 'Unknown date'}")
                        qty_to_sell = 0
                    
                    i += 1
                
                if qty_to_sell > 0:
                    print(f"  Warning: Could not match {qty_to_sell} shares for sale!")
        
        # Add remaining purchases to the result
        for purchase in purchase_queue:
            if purchase['RemainingQty'] > 0:
                remaining_purchases.append(purchase)
    
    # Create DataFrame from remaining purchases
    if remaining_purchases:
        result_df = pd.DataFrame(remaining_purchases)
        
        # Sort by original index to maintain input order
        if 'Original_Index' in result_df.columns:
            result_df = result_df.sort_values('Original_Index')
            result_df = result_df.drop('Original_Index', axis=1)
        
        # Calculate the total cost of remaining shares
        result_df['RemainingCost'] = result_df['RemainingQty'] * result_df['BuyPrice']
        
        # Format dates back to dd/mm/yyyy for output consistency
        result_df['TradeDate'] = result_df['TradeDate'].dt.strftime('%d/%m/%Y')
        
        # Reorder columns for better readability
        columns = ['ScripName', 'Segment', 'TradeDate', 'BuyQty', 'BuyPrice', 'RemainingQty', 'RemainingCost', 'ClientCode', 'OrderNo']
        result_df = result_df[columns]
        
        print(f"\nTotal remaining purchases: {len(result_df)}")
        return result_df
    else:
        print("\nNo remaining purchases found.")
        return pd.DataFrame()

def main():
    # Replace 'sample.csv' with your actual file name
    input_file = 'mishraji_trades.csv'
    
    # Process the trades using FIFO
    remaining_df = process_trades_fifo(input_file)
    
    if not remaining_df.empty:
        # Save the result to a CSV file
        output_file = 'remaining_purchases.csv'
        remaining_df.to_csv(output_file, index=False)
        print(f"Remaining purchases saved to {output_file}")
        
        # Display a summary
        print("\nSummary of remaining purchases by company:")
        
        # For the summary, convert TradeDate back to datetime
        remaining_df['TradeDate'] = pd.to_datetime(remaining_df['TradeDate'], format='%d/%m/%Y', errors='coerce')
        
        summary = remaining_df.groupby('ScripName').agg(
            Total_Remaining_Shares=('RemainingQty', 'sum'),
            Total_Remaining_Cost=('RemainingCost', 'sum'),
            Earliest_Purchase=('TradeDate', 'min'),
            Latest_Purchase=('TradeDate', 'max'),
            Purchases_Count=('RemainingQty', 'count')
        )
        
        # Format dates back to dd/mm/yyyy
        summary['Earliest_Purchase'] = summary['Earliest_Purchase'].dt.strftime('%d/%m/%Y')
        summary['Latest_Purchase'] = summary['Latest_Purchase'].dt.strftime('%d/%m/%Y')
        
        # Calculate average cost per share
        summary['Avg_Cost_Per_Share'] = summary['Total_Remaining_Cost'] / summary['Total_Remaining_Shares']
        
        # Format numeric columns for better readability
        summary['Avg_Cost_Per_Share'] = summary['Avg_Cost_Per_Share'].round(2)
        
        print(summary)
        
        # Save summary to a separate CSV
        summary_file = 'remaining_summary.csv'
        summary.to_csv(summary_file)
        print(f"Summary information saved to {summary_file}")

if __name__ == "__main__":
    main()