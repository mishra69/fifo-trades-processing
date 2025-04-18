# FIFO Trade Processor

This project processes trading data to determine remaining purchases after applying the First-In-First-Out (FIFO) matching method between buys and sells.

## Overview

When transferring between brokerages, you need to know which purchases remain after all sales have been accounted for. This script:

1. Processes your trading data in CSV format
2. Aggregates same-day purchases for each security with weighted average pricing
3. Applies the FIFO method to match sales with purchases
4. Outputs a list of remaining purchases and their quantities
5. Generates a summary of holdings by company

## Requirements

- Python 3.7+
- pandas
- numpy

## Installation

1. Clone or download this repository
2. Create a virtual environment (recommended):
   ```
   python -m venv venv
   ```
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - macOS/Linux: `source venv/bin/activate`
4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Place your trade data CSV file in the same directory as the script
2. By default, the script expects a file named `mishraji_trades.csv`. If your file has a different name, edit the `input_file` variable in the `main()` function
3. Run the script:
   ```
   python fifo_processor.py
   ```
4. Two output files will be created:
   - `remaining_purchases.csv`: All individual purchases with remaining shares
   - `remaining_summary.csv`: Summary by company with total shares and average cost

## Input Format

The script expects a CSV file with the following columns:
- ClientCode: Your client identifier
- TradeDate: Date of the trade in DD/MM/YYYY format
- Segment: Market segment (e.g., NSE, BSE)
- ScripName: Name of the security
- BuyQty: Number of shares purchased (0 if not a buy)
- BuyPrice: Price per share for purchases
- BuyAmount: Total amount for purchases
- SellQty: Number of shares sold (0 if not a sell)
- SellPrice: Price per share for sales
- SellAmount: Total amount for sales
- OrderNo: Order reference number

## Features

- Preserves original order of trades
- Properly handles same-date trades by aggregating buys with weighted average pricing
- Skips processing for securities with trades not in chronological order
- Calculates remaining shares and their cost basis
- Provides summary statistics by company
- Handles various data format issues gracefully

## Same-Day Trade Aggregation

A key feature of this processor is the aggregation of same-day purchases:

- When multiple purchases of the same security occur on the same date, they are combined into a single entry
- The quantity becomes the sum of all purchases for that day
- The price becomes the weighted average price of all purchases for that day
- This simplifies the portfolio and provides a more accurate cost basis

## Output Format

### remaining_purchases.csv
Contains all purchases with shares remaining after FIFO processing:
- ScripName: Company/security name
- Segment: Market segment
- TradeDate: Original purchase date
- BuyQty: Original quantity purchased
- BuyPrice: Purchase price per share (or weighted average for aggregated purchases)
- RemainingQty: Number of shares remaining after FIFO processing
- RemainingCost: Total cost of remaining shares
- ClientCode: Your client identifier
- OrderNo: Original order reference number (or "Aggregated-{date}" for aggregated purchases)
- NumTrades: Number of trades aggregated into this entry

### remaining_summary.csv
Provides aggregated information by company:
- Total_Remaining_Shares: Total shares remaining
- Total_Remaining_Cost: Total cost of remaining shares
- Earliest_Purchase: Date of earliest purchase still holding shares
- Latest_Purchase: Date of latest purchase still holding shares
- Purchases_Count: Number of purchases with remaining shares
- Avg_Cost_Per_Share: Average cost basis per share

## Notes

- The script preserves the original order of trades in your input file
- FIFO matching assumes trades should be processed in chronological order
- The script will skip securities with trades that appear to be out of sequence
- Same-day trades for the same security are consolidated with weighted average pricing