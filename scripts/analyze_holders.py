import pickle
from collections import defaultdict
import pandas as pd
from datetime import datetime

def analyze_holders():
    """
    Analyze the collected transfer data to generate holder statistics
    and other interesting metrics.
    """
    print("Loading transfer data...")
    with open('transfer_data.pkl', 'rb') as f:
        data = pickle.load(f)
        transfers = data['transfers']
        last_block = data['last_block']
    
    print(f"Analyzing {len(transfers):,} transfers up to block {last_block:,}")
    
    # Calculate balances
    balances = defaultdict(float)
    transfer_counts = defaultdict(int)
    volume_by_date = defaultdict(float)
    unique_addresses = set()
    
    for block_num, timestamp, from_addr, to_addr, value in transfers:
        # Update balances
        balances[from_addr] -= value
        balances[to_addr] += value
        
        # Track metrics
        unique_addresses.add(from_addr)
        unique_addresses.add(to_addr)
        
        date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        volume_by_date[date] += value
        transfer_counts[date] += 1
    
    # Prepare holder data
    holder_data = [
        {'address': addr, 'balance': bal}
        for addr, bal in balances.items()
        if bal > 0
    ]
    
    if holder_data:
        df = pd.DataFrame(holder_data)
        df = df.sort_values('balance', ascending=False)
        
        # Calculate statistics
        total_supply = df['balance'].sum()
        top_10_percent = len(df) // 10 if len(df) >= 10 else 1
        concentration_ratio = (df.head(top_10_percent)['balance'].sum() / total_supply) * 100
        
        # Save current holder data
        df.to_csv('current_holders.csv', index=False)
        
        # Save daily statistics
        daily_stats = pd.DataFrame({
            'date': list(volume_by_date.keys()),
            'volume': list(volume_by_date.values()),
            'transfers': list(transfer_counts.values())
        }).sort_values('date')
        daily_stats.to_csv('daily_statistics.csv', index=False)
        
        # Print report
        print("\n=== NFTXBT Token Analysis Report ===")
        print(f"Total Transfers: {len(transfers):,}")
        print(f"Total Holders: {len(df):,}")
        print(f"Total Supply: {total_supply:,.2f}")
        print(f"Average Holdings: {total_supply/len(df):,.2f}")
        print(f"Top 10% Holders Control: {concentration_ratio:.1f}%")
        print(f"Unique Addresses: {len(unique_addresses):,}")
        print("\nTop 5 Holders:")
        print(df.head().to_string())
        
    else:
        print("\nNo holder data collected")

if __name__ == "__main__":
    analyze_holders()