from web3 import Web3
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

# Configuration
RPC_URL = 'https://mainnet.base.org'
w3 = Web3(Web3.HTTPProvider(RPC_URL))
CONTRACT_ADDRESS = '0x08c81699F9a357a9F0d04A09b353576ca328d60D'
DECIMALS = 18
KNOWN_LAUNCH_BLOCK = 23036627

# ABI for tracking transfers
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

contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI)

def get_all_current_holders():
    """
    Get a complete list of all current token holders by processing every transfer
    since the contract's creation. This ensures we don't miss any holders.
    """
    print("Starting comprehensive holder analysis...")
    balances = defaultdict(float)
    processed_transfers = 0
    
    try:
        latest_block = w3.eth.block_number
        print(f"Processing all transfers from launch to current block {latest_block}")
        
        # Use smaller chunks to ensure reliable processing
        chunk_size = 1000
        total_chunks = (latest_block - KNOWN_LAUNCH_BLOCK) // chunk_size + 1
        
        for start in tqdm(range(KNOWN_LAUNCH_BLOCK, latest_block + 1, chunk_size)):
            end_block = min(start + chunk_size - 1, latest_block)
            retries = 3
            
            while retries > 0:
                try:
                    events = contract.events.Transfer.get_logs(
                        from_block=start,
                        to_block=end_block
                    )
                    
                    for event in events:
                        from_addr = event['args']['from']
                        to_addr = event['args']['to']
                        value = float(event['args']['value']) / (10 ** DECIMALS)
                        
                        # Update balances for both addresses involved
                        balances[from_addr] -= value
                        balances[to_addr] += value
                        processed_transfers += 1
                    
                    break  # Success, move to next chunk
                    
                except Exception as e:
                    retries -= 1
                    if retries > 0:
                        print(f"\nRetrying blocks {start} to {end_block}...")
                    else:
                        print(f"\nFailed to process blocks {start} to {end_block}: {e}")
    
        # Create DataFrame with all holders that have a positive balance
        print("\nProcessing final holder balances...")
        holders_data = [
            {'address': addr, 'balance': bal}
            for addr, bal in balances.items()
            if bal > 0.000000001  # Using small threshold to avoid floating point issues
        ]
        
        df = pd.DataFrame(holders_data)
        df = df.sort_values('balance', ascending=False)
        
        # Save all holders to CSV
        output_file = 'all_current_holders.csv'
        df.to_csv(output_file, index=False)
        
        # Print statistics
        print("\n=== Token Holder Analysis ===")
        print(f"Total Current Holders: {len(df):,}")
        print(f"Total Transfers Processed: {processed_transfers:,}")
        print(f"Total Supply: {df['balance'].sum():,.2f}")
        print(f"\nHolder data saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    holders_df = get_all_current_holders()