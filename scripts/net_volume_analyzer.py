#!/usr/bin/env python3
"""
Corrected Net Trading Volume Analyzer for Base Network Tokens
============================================================

Fixes the double-counting issue by properly calculating net trading volume.
Groups transfers by transaction hash to avoid counting multiple transfers 
from the same DEX trade.

Key Improvements:
- Groups transfers by transaction hash
- Calculates net volume per transaction
- Filters by time period (24h/7d/30d options)
- Avoids double counting DEX trades
- Matches DexScreener volume calculation methodology

Author: Token Analysis Tool - Corrected Version
Network: Base Mainnet (Chain ID: 8453)
"""

import sys
import subprocess
import importlib.util
import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

def check_install_package(package_name: str) -> None:
    """Check if a package is installed, if not, install it"""
    if importlib.util.find_spec(package_name) is None:
        print(f"Package {package_name} not found. Installing...")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "--quiet", "--user", package_name
            ])
        except subprocess.CalledProcessError:
            print(f"Warning: Installing with --user failed, trying with --break-system-packages")
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "--quiet", "--break-system-packages", package_name
            ])
        print(f"Package {package_name} has been installed.")

# Check and install required packages
REQUIRED_PACKAGES = ['requests', 'pandas', 'tqdm']
for package in REQUIRED_PACKAGES:
    check_install_package(package)

# API Configuration
ETHERSCAN_API_KEY = "D69IA739C535GRHKHAYE6I1CFFESSRS8WR"
ETHERSCAN_V2_BASE_URL = "https://api.etherscan.io/v2/api"
BASE_CHAIN_ID = 8453

# Target token contracts for analysis
TOKEN_CONTRACTS = [
    "0x0E903C3BBf5ed7179B7D1Da391A3cEEa303134E0",
    "0xF9bc1E5BF79bBF09C940AFc063Ed563B0F4a3c95", 
    "0x43BbFF35b91e721C2359e9f837688cF83B6dBBF1",
    "0x499E5Db42DB458797A8afBaA3bB32B2C46A147fa",
    "0xEA3B0233d176D03d1484d75f46fE0fA611F4B07c"
]

# Zero address for identifying mint/burn transactions
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Time periods for analysis
TIME_PERIODS = {
    '24h': 24 * 3600,
    '7d': 7 * 24 * 3600,
    '30d': 30 * 24 * 3600,
    'all': None
}

if __name__ == "__main__":
    print("🔧 Corrected Net Trading Volume Analyzer")
    print("=" * 50)
    print("🚀 Fixing double-counting issues...")
    print(f"📊 Analyzing {len(TOKEN_CONTRACTS)} tokens")
    print(f"🔗 Network: Base Mainnet (Chain ID: {BASE_CHAIN_ID})")
    print()

class BasescanAPIClient:
    """Client for interacting with Etherscan V2 API for Base network"""
    
    def __init__(self, api_key: str, chain_id: int = BASE_CHAIN_ID):
        self.api_key = api_key
        self.chain_id = chain_id
        self.base_url = ETHERSCAN_V2_BASE_URL
        self.session = requests.Session()
        self.rate_limit_delay = 0.2
        
    def _make_request(self, params: Dict) -> Dict:
        """Make rate-limited API request with error handling"""
        params.update({
            'apikey': self.api_key,
            'chainid': self.chain_id
        })
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(self.rate_limit_delay)
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if data.get('status') == '1':
                    return data
                elif 'rate limit' in data.get('message', '').lower():
                    print(f"⚠️  Rate limit hit, waiting 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    print(f"⚠️  API Warning: {data.get('message', 'Unknown error')}")
                    return data
                    
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    print(f"❌ API request failed after {max_retries} attempts: {e}")
                    return {'status': '0', 'message': str(e), 'result': []}
                time.sleep(2 ** attempt)
        
        return {'status': '0', 'message': 'Max retries exceeded', 'result': []}
    
    def get_token_transfers(self, contract_address: str, start_block: int = 0, 
                          end_block: int = 99999999, page: int = 1, 
                          offset: int = 10000) -> List[Dict]:
        """Get ERC-20 token transfer events"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'page': page,
            'offset': offset,
            'sort': 'desc'  # Get newest first
        }
        
        response = self._make_request(params)
        return response.get('result', [])
    
    def get_current_token_price_usd(self, contract_address: str) -> Optional[float]:
        """Get current token price in USD from DexScreener"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get('pairs', [])
            
            # Find Base network pairs
            base_pairs = [p for p in pairs if p.get('chainId') == 'base']
            if not base_pairs:
                base_pairs = pairs
                
            if base_pairs:
                # Get the pair with highest liquidity
                best_pair = max(base_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                price_usd = best_pair.get('priceUsd')
                if price_usd:
                    return float(price_usd)
                    
        except Exception as e:
            print(f"⚠️  Price fetch failed for {contract_address[:10]}...: {e}")
            
        return None

class TokenConfig:
    """Configuration and metadata for a token"""
    
    def __init__(self, address: str, decimals: int = 18):
        self.address = address.lower()
        self.decimals = decimals
        
    def format_amount(self, raw_amount: str) -> float:
        """Convert raw token amount to decimal format"""
        try:
            return float(raw_amount) / (10 ** self.decimals)
        except (ValueError, TypeError):
            return 0.0

class NetVolumeAnalyzer:
    """Corrected volume analyzer that properly calculates net trading volume"""
    
    def __init__(self, api_key: str):
        self.api_client = BasescanAPIClient(api_key)
        self.tokens = {}
        
    def setup_tokens(self, contract_addresses: List[str]) -> None:
        """Initialize token configurations"""
        print("🔧 Setting up token configurations...")
        
        for address in contract_addresses:
            # For this corrected version, we'll use default 18 decimals
            # In production, you'd want to fetch this from the contract
            self.tokens[address.lower()] = TokenConfig(address, 18)
            print(f"  📋 {address[:10]}... configured")
                
        print(f"✅ Configured {len(self.tokens)} tokens\n")
    
    def fetch_recent_transfers(self, contract_address: str, hours: int = 24) -> List[Dict]:
        """Fetch transfers from the last N hours"""
        print(f"📥 Fetching {hours}h transfers for {contract_address[:10]}...")
        
        cutoff_time = int(time.time()) - (hours * 3600) if hours else 0
        all_transfers = []
        page = 1
        max_pages = 50  # Safety limit
        
        with tqdm(desc=f"Loading transfers", unit="page") as pbar:
            while page <= max_pages:
                transfers = self.api_client.get_token_transfers(
                    contract_address, page=page, offset=10000
                )
                
                if not transfers or len(transfers) == 0:
                    break
                
                # Filter by time period and exclude mint/burn
                recent_transfers = []
                page_has_recent = False
                
                for tx in transfers:
                    timestamp = int(tx.get('timeStamp', 0))
                    
                    # Check if this transfer is within our time window
                    if not hours or timestamp >= cutoff_time:
                        page_has_recent = True
                        # Exclude mint/burn transactions
                        if (tx.get('from', '').lower() != ZERO_ADDRESS.lower() and 
                            tx.get('to', '').lower() != ZERO_ADDRESS.lower()):
                            recent_transfers.append(tx)
                
                # If no transfers on this page are recent, we can stop
                if hours and not page_has_recent:
                    print(f"\n  ⏰ Reached {hours}h cutoff at page {page}")
                    break
                
                all_transfers.extend(recent_transfers)
                pbar.update(1)
                pbar.set_postfix({
                    'Total': len(all_transfers),
                    'Page': page,
                    'Recent': len(recent_transfers)
                })
                
                # If we got less than the max per page, we're done
                if len(transfers) < 10000:
                    break
                    
                page += 1
        
        print(f"  ✅ Found {len(all_transfers):,} recent transfers\n")
        return all_transfers

    def calculate_net_volume(self, contract_address: str, transfers: List[Dict], 
                           period_name: str = "24h") -> Dict:
        """Calculate net trading volume by grouping transfers by transaction"""
        token = self.tokens[contract_address.lower()]
        print(f"📊 Calculating net volume for {contract_address[:10]}... ({period_name})")
        
        if not transfers:
            return {
                'contract_address': contract_address,
                'period': period_name,
                'total_transfers': 0,
                'unique_transactions': 0,
                'net_volume_native': 0.0,
                'net_volume_usd': 0.0,
                'current_price_usd': 0.0,
                'unique_addresses': 0,
                'first_transfer': None,
                'last_transfer': None
            }
        
        # Group transfers by transaction hash
        tx_groups = defaultdict(list)
        for transfer in transfers:
            tx_hash = transfer.get('hash', '')
            if tx_hash:
                tx_groups[tx_hash].append(transfer)
        
        print(f"  📝 {len(transfers):,} transfers in {len(tx_groups):,} transactions")
        
        # Calculate net volume per transaction
        net_volume_native = 0.0
        unique_addresses = set()
        transaction_volumes = []
        
        for tx_hash, tx_transfers in tx_groups.items():
            # For each transaction, calculate the net trading volume
            # We'll use the largest transfer in the transaction as the trade size
            # This helps avoid counting fees and internal transfers
            
            tx_amounts = []
            for transfer in tx_transfers:
                amount = token.format_amount(transfer.get('value', '0'))
                tx_amounts.append(amount)
                
                # Track addresses
                unique_addresses.add(transfer.get('from', '').lower())
                unique_addresses.add(transfer.get('to', '').lower())
            
            # Use the largest transfer as the primary trade volume
            # This avoids double counting fees and internal transfers
            if tx_amounts:
                primary_volume = max(tx_amounts)
                net_volume_native += primary_volume
                
                transaction_volumes.append({
                    'tx_hash': tx_hash,
                    'volume': primary_volume,
                    'transfer_count': len(tx_transfers),
                    'timestamp': int(tx_transfers[0].get('timeStamp', 0))
                })
        
        # Get current price and calculate USD volume
        current_price_usd = self.api_client.get_current_token_price_usd(contract_address)
        net_volume_usd = net_volume_native * current_price_usd if current_price_usd else 0.0
        
        # Time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in transfers if tx.get('timeStamp')]
        first_transfer = min(timestamps) if timestamps else None
        last_transfer = max(timestamps) if timestamps else None
        
        results = {
            'contract_address': contract_address,
            'period': period_name,
            'total_transfers': len(transfers),
            'unique_transactions': len(tx_groups),
            'net_volume_native': net_volume_native,
            'net_volume_usd': net_volume_usd,
            'current_price_usd': current_price_usd,
            'unique_addresses': len(unique_addresses),
            'first_transfer': datetime.fromtimestamp(first_transfer) if first_transfer else None,
            'last_transfer': datetime.fromtimestamp(last_transfer) if last_transfer else None,
            'transaction_volumes': sorted(transaction_volumes, key=lambda x: x['volume'], reverse=True)[:10]
        }
        
        print(f"  ✅ Net Volume: {net_volume_native:,.2f} tokens")
        if current_price_usd:
            print(f"  💰 USD Value: ${net_volume_usd:,.2f}")
        print(f"  📈 Transactions: {len(tx_groups):,} (from {len(transfers):,} transfers)")
        print(f"  👥 Unique Addresses: {len(unique_addresses):,}")
        
        # Calculate reduction factor
        old_method_volume = sum(token.format_amount(tx.get('value', '0')) for tx in transfers)
        reduction_factor = old_method_volume / net_volume_native if net_volume_native > 0 else 0
        print(f"  📉 Volume Reduction: {reduction_factor:.2f}x (from ${old_method_volume * current_price_usd:,.2f} to ${net_volume_usd:,.2f})")
        print()
        
        return results
    
    def analyze_token_periods(self, contract_address: str) -> Dict:
        """Analyze token volume across different time periods"""
        print(f"🚀 Analyzing {contract_address[:10]}... across multiple periods\n")
        
        results = {}
        
        # Analyze different time periods
        for period_name, hours in [('24h', 24), ('7d', 168), ('30d', 720)]:
            try:
                transfers = self.fetch_recent_transfers(contract_address, hours)
                volume_data = self.calculate_net_volume(contract_address, transfers, period_name)
                results[period_name] = volume_data
                
            except Exception as e:
                print(f"❌ Error analyzing {period_name} for {contract_address[:10]}...: {e}")
                results[period_name] = {
                    'error': str(e),
                    'net_volume_usd': 0.0
                }
        
        return results
    
    def compare_with_dexscreener(self, contract_address: str) -> Dict:
        """Compare our results with DexScreener data"""
        print(f"🔍 Comparing with DexScreener for {contract_address[:10]}...")
        
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get('pairs', [])
            
            if pairs:
                # Get the main trading pair
                main_pair = max(pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                
                dex_data = {
                    'token_symbol': main_pair.get('baseToken', {}).get('symbol', 'Unknown'),
                    'price_usd': float(main_pair.get('priceUsd', 0)),
                    'volume_24h': float(main_pair.get('volume', {}).get('h24', 0)),
                    'txns_24h': main_pair.get('txns', {}).get('h24', {}),
                    'pair_address': main_pair.get('pairAddress', ''),
                    'dex': main_pair.get('dexId', '')
                }
                
                print(f"  📊 DexScreener 24h Volume: ${dex_data['volume_24h']:,.2f}")
                print(f"  💱 Price: ${dex_data['price_usd']:.6f}")
                print(f"  🔄 24h Transactions: {dex_data['txns_24h']}")
                print()
                
                return dex_data
                
        except Exception as e:
            print(f"⚠️  DexScreener comparison failed: {e}")
            
        return {}

def main():
    """Main execution function"""
    print("🔧 Net Trading Volume Analyzer - Corrected Version")
    print("=" * 60)
    print("🎯 Goal: Fix double-counting and match DexScreener volumes")
    print()
    
    # Initialize analyzer
    analyzer = NetVolumeAnalyzer(ETHERSCAN_API_KEY)
    
    # Setup tokens
    analyzer.setup_tokens(TOKEN_CONTRACTS)
    
    # Focus on the problematic token first
    target_token = "0xEA3B0233d176D03d1484d75f46fE0fA611F4B07c"
    
    print("="*60)
    print(f"🎯 FOCUSED ANALYSIS: {target_token}")
    print("="*60)
    
    try:
        # Get DexScreener reference data
        dex_data = analyzer.compare_with_dexscreener(target_token)
        
        # Analyze our corrected calculation
        period_results = analyzer.analyze_token_periods(target_token)
        
        # Print comparison
        print("📊 VOLUME COMPARISON")
        print("-" * 40)
        
        if dex_data and '24h' in period_results:
            our_24h = period_results['24h']['net_volume_usd']
            dex_24h = dex_data['volume_24h']
            
            print(f"DexScreener 24h:  ${dex_24h:,.2f}")
            print(f"Our Corrected:    ${our_24h:,.2f}")
            
            if dex_24h > 0:
                accuracy = (our_24h / dex_24h) * 100
                print(f"Accuracy:         {accuracy:.1f}%")
                
                if 80 <= accuracy <= 120:
                    print("✅ Volume calculation looks accurate!")
                else:
                    print("⚠️  Still some discrepancy - may need further refinement")
            
        print("\n📈 MULTI-PERIOD RESULTS")
        print("-" * 40)
        for period, data in period_results.items():
            if 'error' not in data:
                print(f"{period:>4}: ${data['net_volume_usd']:>10,.2f} "
                      f"({data['unique_transactions']:,} txns)")
        
        # Export detailed results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/csv_outputs/net_volume_analysis_{timestamp}.csv"
        
        # Create export data
        export_data = []
        for period, data in period_results.items():
            if 'error' not in data:
                export_data.append({
                    'Contract_Address': target_token,
                    'Token_Symbol': dex_data.get('token_symbol', 'UNKNOWN'),
                    'Period': period,
                    'Net_Volume_USD': f"{data['net_volume_usd']:.2f}",
                    'Net_Volume_Native': f"{data['net_volume_native']:.6f}",
                    'Unique_Transactions': data['unique_transactions'],
                    'Total_Transfers': data['total_transfers'],
                    'Current_Price_USD': f"{data['current_price_usd']:.8f}",
                    'Unique_Addresses': data['unique_addresses'],
                    'Analysis_Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        if export_data:
            df = pd.DataFrame(export_data)
            import os
            os.makedirs("data/csv_outputs", exist_ok=True)
            df.to_csv(filename, index=False)
            print(f"\n📄 Results exported to: {filename}")
        
        print("\n✅ Corrected analysis complete!")
        
    except KeyboardInterrupt:
        print("\n❌ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 