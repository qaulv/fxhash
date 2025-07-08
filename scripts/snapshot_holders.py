#!/usr/bin/env python3
import sys
import subprocess
import importlib.util
import pip

def check_install_package(package_name):
    """Check if a package is installed, if not, install it"""
    if importlib.util.find_spec(package_name) is None:
        print(f"Package {package_name} not found. Installing...")
        try:
            # Try with --user flag first (recommended for Homebrew Python)
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "--user", package_name])
        except subprocess.CalledProcessError:
            # If that fails, try with --break-system-packages as fallback
            print(f"Warning: Installing with --user failed, trying with --break-system-packages")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "--break-system-packages", package_name])
        print(f"Package {package_name} has been installed.")
    else:
        print(f"Package {package_name} is already installed.")

# Check and install required packages
required_packages = ['web3', 'pandas', 'tqdm']
for package in required_packages:
    check_install_package(package)

# Now import the required packages
from web3 import Web3
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime

# Set up our connection to Base network
RPC_URL = 'https://mainnet.base.org'
w3 = Web3(Web3.HTTPProvider(RPC_URL))
CONTRACT_ADDRESS = '0x5Fc2843838e65eb0B5d33654628F446d54602791'
DECIMALS = 18

# We only need the Transfer event definition
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

def get_current_holders():
    """Get the current state of all token holders as efficiently as possible"""
    start_time = datetime.now()
    print(f"Starting holder analysis at {start_time.strftime('%Y-%m-%d %H:%M')}...")
    balances = defaultdict(float)
    
    try:
        # Get the latest block once - this is our snapshot point
        latest_block = w3.eth.block_number
        print(f"Processing transfers up to block {latest_block}")
        
        # Process blocks in larger chunks since we only care about final state
        chunk_size = 2000
        start_block = 23036627  # Your token's launch block
        
        # Calculate number of chunks for our progress bar
        total_chunks = (latest_block - start_block) // chunk_size + 1
        
        for start in tqdm(range(start_block, latest_block + 1, chunk_size), total=total_chunks):
            end_block = min(start + chunk_size - 1, latest_block)
            
            try:
                # Get all transfers in this chunk
                events = contract.events.Transfer.get_logs(
                    from_block=start,
                    to_block=end_block
                )
                
                # Update balances with these transfers
                for event in events:
                    from_addr = event['args']['from']
                    to_addr = event['args']['to']
                    value = float(event['args']['value']) / (10 ** DECIMALS)
                    
                    balances[from_addr] -= value
                    balances[to_addr] += value
                    
            except Exception as e:
                print(f"\nError processing blocks {start} to {end_block}: {e}")
                continue
        
        # Create our final holders data
        print("\nPreparing final holder analysis...")
        holders_data = [
            {'address': addr, 'balance': bal}
            for addr, bal in balances.items()
            if bal > 0  # Only include addresses with positive balances
        ]
        
        if holders_data:
            df = pd.DataFrame(holders_data)
            df = df.sort_values('balance', ascending=False)
            
            # Create timestamp for filename and save
            timestamp = datetime.now().strftime('%Y-%m-%d_%H%M')
            output_file = f'/Users/paulschmidt/Desktop/{timestamp}_current_holders.csv'
            df.to_csv(output_file, index=False)
            
            # Calculate key metrics
            total_supply = df['balance'].sum()
            end_time = datetime.now()
            
            print("\n=== Token Holder Analysis ===")
            print(f"Analysis timestamp: {timestamp}")
            print(f"Analysis duration: {(end_time - start_time).total_seconds() / 60:.2f} minutes")
            print(f"Current block: {latest_block}")
            print(f"Total Holders: {len(df):,}")
            print(f"Total Supply: {total_supply:,.2f}")
            print(f"Average Balance: {total_supply/len(df):,.2f}")
            
            print(f"\nHolder data saved to {output_file}")
            
            # Show top holders
            print("\nTop 10 Holders:")
            print(df.head(10).to_string())
            
            return df, latest_block
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

if __name__ == "__main__":
    try:
        print("Token Holder Analysis Tool")
        print("==========================")
        holders_df, snapshot_block = get_current_holders()
        
        # Wait for user input before closing
        input("\nPress Enter to exit...")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        input("\nPress Enter to exit...")