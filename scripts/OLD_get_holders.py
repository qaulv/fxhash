from web3 import Web3
import pandas as pd
from collections import defaultdict
import time
from tqdm import tqdm
from datetime import datetime

# Configuration for our blockchain connection
RPC_URL = 'https://mainnet.base.org'
w3 = Web3(Web3.HTTPProvider(RPC_URL))
CONTRACT_ADDRESS = '0x08c81699F9a357a9F0d04A09b353576ca328d60D'
DECIMALS = 18
KNOWN_LAUNCH_BLOCK = 23036627  # The exact block where we found the first transfer

# Our contract's ABI, focused on Transfer events
ABI = '''[{
    "anonymous": false,
    "inputs": [
        {"indexed": true, "name": "from", "type": "address"},
        {"indexed": true, "name": "to", "type": "address"},
        {"indexed": false, "name": "value", "type": "uint256"}
    ],
    "name": "Transfer",
    "type": "event"
}]'''

# Initialize our contract interface
contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI)

def analyze_transfer_patterns(events):
    """
    Analyze transfer patterns to provide insights about token movement.
    This helps understand how the token is being used and distributed.
    """
    transfer_counts = defaultdict(int)
    volume_by_date = defaultdict(float)
    unique_addresses = set()

    for event in events:
        # Track unique addresses involved in transfers
        from_addr = event['args']['from']
        to_addr = event['args']['to']
        unique_addresses.add(from_addr)
        unique_addresses.add(to_addr)

        # Calculate transfer amount in tokens
        value = float(event['args']['value']) / (10 ** DECIMALS)
        
        # Get timestamp for this transfer
        block = w3.eth.get_block(event['blockNumber'])
        date = datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d')
        
        # Track daily volumes
        volume_by_date[date] += value
        transfer_counts[date] += 1

    return {
        'unique_addresses': len(unique_addresses) - 1,  # Subtract 1 to exclude zero address
        'daily_volumes': dict(volume_by_date),
        'daily_transfers': dict(transfer_counts)
    }

def get_token_holders():
    """
    Calculate current token holder balances and provide detailed analytics 
    about token distribution and transfer patterns.
    """
    balances = defaultdict(float)
    all_events = []  # Store all events for analysis
    
    try:
        latest_block = w3.eth.block_number
        chunk_size = 1000
        
        print(f"\nProcessing transfers from launch block {KNOWN_LAUNCH_BLOCK} to current block {latest_block}")
        print(f"Analyzing {latest_block - KNOWN_LAUNCH_BLOCK:,} blocks of token history...")
        
        # Process blocks with progress tracking
        chunks = range(KNOWN_LAUNCH_BLOCK, latest_block + 1, chunk_size)
        for start in tqdm(chunks, desc="Processing blocks"):
            end_block = min(start + chunk_size - 1, latest_block)
            retries = 3
            
            while retries > 0:
                try:
                    events = contract.events.Transfer.get_logs(
                        from_block=start,
                        to_block=end_block
                    )
                    
                    # Update balances and collect events for analysis
                    for event in events:
                        from_addr = event['args']['from']
                        to_addr = event['args']['to']
                        value = float(event['args']['value']) / (10 ** DECIMALS)
                        
                        balances[from_addr] -= value
                        balances[to_addr] += value
                        all_events.append(event)
                    
                    break  # Success, move to next chunk
                    
                except Exception as e:
                    retries -= 1
                    if retries > 0:
                        time.sleep(1)
                    else:
                        print(f"\nError processing blocks {start} to {end_block}: {e}")
        
        # Prepare holder data
        print("\nAnalyzing holder distribution...")
        holder_data = [
            {'address': addr, 'balance': bal}
            for addr, bal in balances.items()
            if bal > 0
        ]
        
        if holder_data:
            # Create our main holder DataFrame
            df = pd.DataFrame(holder_data)
            df = df.sort_values('balance', ascending=False)
            
            # Calculate some interesting statistics
            total_supply = df['balance'].sum()
            top_10_percent = len(df) // 10 if len(df) >= 10 else 1
            concentration_ratio = (df.head(top_10_percent)['balance'].sum() / total_supply) * 100
            
            # Analyze transfer patterns
            transfer_analysis = analyze_transfer_patterns(all_events)
            
            # Save detailed holder data
            output_file = 'token_holders.csv'
            df.to_csv(output_file, index=False)
            
            # Generate analysis report
            print("\n=== NFTXBT Token Analysis Report ===")
            print(f"Total Holders: {len(df):,}")
            print(f"Total Supply in Circulation: {total_supply:,.2f}")
            print(f"Average Holdings: {total_supply/len(df):,.2f}")
            print(f"Top 10% Holders Control: {concentration_ratio:.1f}%")
            print(f"Unique Addresses Involved: {transfer_analysis['unique_addresses']:,}")
            print("\nTop 5 Holders:")
            print(df.head().to_string())
            
            # Save daily statistics
            daily_stats = pd.DataFrame({
                'date': list(transfer_analysis['daily_volumes'].keys()),
                'volume': list(transfer_analysis['daily_volumes'].values()),
                'transfers': list(transfer_analysis['daily_transfers'].values())
            })
            daily_stats.to_csv('daily_statistics.csv', index=False)
            print(f"\nDetailed daily statistics saved to 'daily_statistics.csv'")
            
        else:
            print("\nNo holder data collected")
            
    except Exception as e:
        print(f"\nScript error: {e}")

if __name__ == "__main__":
    print("Starting NFTXBT token analysis...")
    get_token_holders()