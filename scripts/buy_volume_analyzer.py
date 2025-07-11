#!/usr/bin/env python3
"""
Buy Volume Analyzer for Base Network Tokens
===========================================

CORRECTLY calculates buy volume by:
1. Identifying DEX pool addresses
2. Only counting BUY transactions (tokens FROM pool TO users)
3. Calculating total historical buy volume in USD
4. Cross-checking methodology with DexScreener 24h volume

Key Logic:
- BUY = tokens flow FROM pool TO user (we count this)
- SELL = tokens flow FROM user TO pool (we ignore this)
- Volume = SUM(all buy transactions in USD)

Author: Token Analysis Tool - Buy Volume Focus
Network: Base Mainnet (Chain ID: 8453)
"""

import sys
import subprocess
import importlib.util
import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
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

if __name__ == "__main__":
    print("🛒 Buy Volume Analyzer")
    print("=" * 50)
    print("🎯 Only counting BUY transactions (pool → users)")
    print(f"📊 Analyzing {len(TOKEN_CONTRACTS)} tokens")
    print(f"🔗 Network: Base Mainnet (Chain ID: {BASE_CHAIN_ID})")
    print()

class DexScreenerClient:
    """Client for DexScreener API to get pool info and validation data"""
    
    def get_token_info(self, contract_address: str) -> Dict:
        """Get token info including pool addresses and current price"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get('pairs', [])
            
            # Find Base network pairs
            base_pairs = [p for p in pairs if p.get('chainId') == 'base']
            if not base_pairs:
                return {}
                
            # Get the main trading pair (highest liquidity)
            main_pair = max(base_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
            
            return {
                'token_symbol': main_pair.get('baseToken', {}).get('symbol', 'Unknown'),
                'token_name': main_pair.get('baseToken', {}).get('name', 'Unknown'),
                'price_usd': float(main_pair.get('priceUsd', 0)),
                'volume_24h': float(main_pair.get('volume', {}).get('h24', 0)),
                'pool_address': main_pair.get('pairAddress', '').lower(),
                'quote_token': main_pair.get('quoteToken', {}).get('symbol', 'Unknown'),
                'dex': main_pair.get('dexId', 'Unknown'),
                'txns_24h': main_pair.get('txns', {}).get('h24', {}),
                'all_pools': [p.get('pairAddress', '').lower() for p in base_pairs if p.get('pairAddress')]
            }
            
        except Exception as e:
            print(f"⚠️  DexScreener data fetch failed for {contract_address[:10]}...: {e}")
            return {}

class BasescanAPIClient:
    """Client for Etherscan V2 API for Base network"""
    
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
    
    def get_all_token_transfers(self, contract_address: str) -> List[Dict]:
        """Get ALL transfer events for a token (complete history)"""
        print(f"📥 Fetching ALL transfers for {contract_address[:10]}...")
        
        all_transfers = []
        page = 1
        max_pages = 200  # Safety limit
        
        with tqdm(desc=f"Loading all transfers", unit="page") as pbar:
            while page <= max_pages:
                params = {
                    'module': 'account',
                    'action': 'tokentx',
                    'contractaddress': contract_address,
                    'startblock': 0,
                    'endblock': 99999999,
                    'page': page,
                    'offset': 10000,
                    'sort': 'asc'  # Get oldest first for complete history
                }
                
                response = self._make_request(params)
                transfers = response.get('result', [])
                
                if not transfers or len(transfers) == 0:
                    break
                
                # Filter out mint/burn transactions
                valid_transfers = [
                    tx for tx in transfers 
                    if tx.get('from', '').lower() != ZERO_ADDRESS.lower() 
                    and tx.get('to', '').lower() != ZERO_ADDRESS.lower()
                ]
                
                all_transfers.extend(valid_transfers)
                pbar.update(1)
                pbar.set_postfix({
                    'Total': len(all_transfers),
                    'Page': page,
                    'Valid': len(valid_transfers),
                    'Raw': len(transfers)
                })
                
                # If we got less than the max per page, we're done
                if len(transfers) < 10000:
                    break
                    
                page += 1
        
        print(f"  ✅ Found {len(all_transfers):,} total valid transfers\n")
        return all_transfers

class TokenConfig:
    """Configuration and metadata for a token"""
    
    def __init__(self, address: str, dex_info: Dict, decimals: int = 18):
        self.address = address.lower()
        self.decimals = decimals
        self.symbol = dex_info.get('token_symbol', f'TKN_{address[:6]}')
        self.name = dex_info.get('token_name', f'Token_{address[:8]}')
        self.price_usd = dex_info.get('price_usd', 0.0)
        self.pool_addresses = set(dex_info.get('all_pools', []))
        self.main_pool = dex_info.get('pool_address', '')
        self.dex_volume_24h = dex_info.get('volume_24h', 0.0)
        
    def format_amount(self, raw_amount: str) -> float:
        """Convert raw token amount to decimal format"""
        try:
            return float(raw_amount) / (10 ** self.decimals)
        except (ValueError, TypeError):
            return 0.0
    
    def is_pool_address(self, address: str) -> bool:
        """Check if an address is a known DEX pool"""
        return address.lower() in self.pool_addresses
    
    def __str__(self):
        return f"{self.symbol} - {self.name} (${self.price_usd:.6f})"

class BuyVolumeAnalyzer:
    """Analyzer that only counts BUY volume (tokens FROM pools TO users)"""
    
    def __init__(self, api_key: str):
        self.etherscan_client = BasescanAPIClient(api_key)
        self.dexscreener_client = DexScreenerClient()
        self.tokens = {}
        
    def setup_token(self, contract_address: str) -> bool:
        """Setup a single token with DEX pool information"""
        print(f"🔧 Setting up {contract_address[:10]}...")
        
        # Get DEX info first
        dex_info = self.dexscreener_client.get_token_info(contract_address)
        if not dex_info:
            print(f"  ❌ Could not fetch DEX info")
            return False
            
        # Create token config
        self.tokens[contract_address.lower()] = TokenConfig(contract_address, dex_info)
        token = self.tokens[contract_address.lower()]
        
        print(f"  ✅ {token}")
        print(f"  🏊 Pool: {token.main_pool}")
        print(f"  📊 DexScreener 24h: ${token.dex_volume_24h:,.2f}")
        print()
        
        return True
    
    def calculate_buy_volume(self, contract_address: str) -> Dict:
        """Calculate total historical BUY volume only"""
        token = self.tokens[contract_address.lower()]
        print(f"🛒 Calculating BUY volume for {token.symbol}...")
        
        # Get all transfers
        all_transfers = self.etherscan_client.get_all_token_transfers(contract_address)
        
        if not all_transfers:
            return self._empty_result(contract_address)
        
        # Separate buys from sells
        buy_transfers = []
        sell_transfers = []
        other_transfers = []
        
        for transfer in all_transfers:
            from_addr = transfer.get('from', '').lower()
            to_addr = transfer.get('to', '').lower()
            
            if token.is_pool_address(from_addr):
                # Tokens flowing FROM pool TO user = BUY
                buy_transfers.append(transfer)
            elif token.is_pool_address(to_addr):
                # Tokens flowing FROM user TO pool = SELL
                sell_transfers.append(transfer)
            else:
                # Other transfers (airdrops, transfers between users, etc.)
                other_transfers.append(transfer)
        
        # Calculate buy volume
        total_buy_volume_native = sum(token.format_amount(tx.get('value', '0')) for tx in buy_transfers)
        total_buy_volume_usd = total_buy_volume_native * token.price_usd
        
        # Calculate sell volume for comparison
        total_sell_volume_native = sum(token.format_amount(tx.get('value', '0')) for tx in sell_transfers)
        total_sell_volume_usd = total_sell_volume_native * token.price_usd
        
        # Get time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in all_transfers if tx.get('timeStamp')]
        first_transfer = min(timestamps) if timestamps else None
        last_transfer = max(timestamps) if timestamps else None
        
        # Calculate 24h buy volume for validation
        current_time = int(time.time())
        h24_cutoff = current_time - (24 * 3600)
        
        h24_buys = [tx for tx in buy_transfers if int(tx.get('timeStamp', 0)) >= h24_cutoff]
        h24_buy_volume_native = sum(token.format_amount(tx.get('value', '0')) for tx in h24_buys)
        h24_buy_volume_usd = h24_buy_volume_native * token.price_usd
        
        # Unique addresses
        buy_addresses = set(tx.get('to', '') for tx in buy_transfers)
        sell_addresses = set(tx.get('from', '') for tx in sell_transfers)
        unique_buyers = len(buy_addresses)
        unique_sellers = len(sell_addresses)
        
        results = {
            'contract_address': contract_address,
            'token': token,
            
            # BUY VOLUME (what we care about)
            'total_buy_volume_native': total_buy_volume_native,
            'total_buy_volume_usd': total_buy_volume_usd,
            'total_buy_transfers': len(buy_transfers),
            'unique_buyers': unique_buyers,
            
            # SELL VOLUME (for comparison)
            'total_sell_volume_native': total_sell_volume_native,
            'total_sell_volume_usd': total_sell_volume_usd,
            'total_sell_transfers': len(sell_transfers),
            'unique_sellers': unique_sellers,
            
            # 24H VALIDATION
            'h24_buy_volume_usd': h24_buy_volume_usd,
            'h24_buy_transfers': len(h24_buys),
            'dexscreener_24h_volume': token.dex_volume_24h,
            
            # OTHER
            'other_transfers': len(other_transfers),
            'total_transfers': len(all_transfers),
            'current_price_usd': token.price_usd,
            'first_transfer': datetime.fromtimestamp(first_transfer) if first_transfer else None,
            'last_transfer': datetime.fromtimestamp(last_transfer) if last_transfer else None,
            
            # Top buy transactions
            'top_buys': sorted([
                {
                    'amount_native': token.format_amount(tx.get('value', '0')),
                    'amount_usd': token.format_amount(tx.get('value', '0')) * token.price_usd,
                    'buyer': tx.get('to', ''),
                    'tx_hash': tx.get('hash', ''),
                    'timestamp': int(tx.get('timeStamp', 0))
                }
                for tx in buy_transfers
            ], key=lambda x: x['amount_usd'], reverse=True)[:10]
        }
        
        self._print_analysis_results(results)
        return results
    
    def _empty_result(self, contract_address: str) -> Dict:
        """Return empty result structure"""
        return {
            'contract_address': contract_address,
            'total_buy_volume_usd': 0.0,
            'total_buy_transfers': 0,
            'error': 'No transfers found'
        }
    
    def _print_analysis_results(self, results: Dict) -> None:
        """Print detailed analysis results"""
        token = results['token']
        
        print(f"  🛒 TOTAL BUY VOLUME: {results['total_buy_volume_native']:,.2f} {token.symbol}")
        print(f"  💰 TOTAL BUY USD: ${results['total_buy_volume_usd']:,.2f}")
        print(f"  📈 Buy Transfers: {results['total_buy_transfers']:,}")
        print(f"  👥 Unique Buyers: {results['unique_buyers']:,}")
        print()
        
        print(f"  📉 Sell Volume: {results['total_sell_volume_native']:,.2f} {token.symbol} (${results['total_sell_volume_usd']:,.2f})")
        print(f"  📈 Sell Transfers: {results['total_sell_transfers']:,}")
        print(f"  👥 Unique Sellers: {results['unique_sellers']:,}")
        print()
        
        # 24h validation
        print(f"  🕐 24h Buy Volume: ${results['h24_buy_volume_usd']:,.2f}")
        print(f"  🔍 DexScreener 24h: ${results['dexscreener_24h_volume']:,.2f}")
        
        if results['dexscreener_24h_volume'] > 0:
            accuracy = (results['h24_buy_volume_usd'] / results['dexscreener_24h_volume']) * 100
            print(f"  ✅ 24h Accuracy: {accuracy:.1f}%")
            
            if 80 <= accuracy <= 120:
                print(f"  🎯 Methodology validated!")
            else:
                print(f"  ⚠️  Methodology needs refinement")
        
        # Token age
        if results.get('first_transfer') and results.get('last_transfer'):
            age_hours = (results['last_transfer'] - results['first_transfer']).total_seconds() / 3600
            print(f"  🕐 Token Age: {age_hours:.1f} hours")
        
        print()
    
    def export_results(self, results: Dict, output_dir: str = "data/csv_outputs") -> str:
        """Export buy volume analysis to CSV"""
        print("📄 Exporting buy volume analysis...")
        
        # Prepare CSV data
        csv_data = {
            'Contract_Address': results['contract_address'],
            'Token_Symbol': results['token'].symbol,
            'Token_Name': results['token'].name,
            'Current_Price_USD': f"{results['current_price_usd']:.8f}",
            
            # BUY VOLUME (main metrics)
            'Total_Buy_Volume_USD': f"{results['total_buy_volume_usd']:.2f}",
            'Total_Buy_Volume_Native': f"{results['total_buy_volume_native']:.6f}",
            'Total_Buy_Transfers': results['total_buy_transfers'],
            'Unique_Buyers': results['unique_buyers'],
            
            # VALIDATION
            'H24_Buy_Volume_USD': f"{results['h24_buy_volume_usd']:.2f}",
            'DexScreener_24h_Volume': f"{results['dexscreener_24h_volume']:.2f}",
            'Validation_Accuracy_Pct': f"{(results['h24_buy_volume_usd'] / results['dexscreener_24h_volume'] * 100) if results['dexscreener_24h_volume'] > 0 else 0:.1f}",
            
            # ADDITIONAL INFO
            'Total_Sell_Volume_USD': f"{results['total_sell_volume_usd']:.2f}",
            'Other_Transfers': results['other_transfers'],
            'Token_Age_Hours': f"{(results['last_transfer'] - results['first_transfer']).total_seconds() / 3600 if results.get('first_transfer') and results.get('last_transfer') else 0:.1f}",
            'Analysis_Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Create DataFrame and export
        df = pd.DataFrame([csv_data])
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/buy_volume_analysis_{timestamp}.csv"
        
        # Ensure directory exists
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        df.to_csv(filename, index=False)
        
        print(f"  ✅ Results exported to: {filename}")
        return filename

def main():
    """Main execution function"""
    print("🛒 Buy Volume Analyzer - Corrected Logic")
    print("=" * 60)
    print("🎯 Goal: Count only BUY volume (pool → users)")
    print("🔍 Validate methodology with DexScreener 24h volume")
    print()
    
    # Initialize analyzer
    analyzer = BuyVolumeAnalyzer(ETHERSCAN_API_KEY)
    
    # Focus on GALO token for validation
    target_token = "0xEA3B0233d176D03d1484d75f46fE0fA611F4B07c"
    
    print("="*60)
    print(f"🎯 FOCUSED ANALYSIS: GALO Token")
    print("="*60)
    
    try:
        # Setup token with DEX info
        if not analyzer.setup_token(target_token):
            print("❌ Failed to setup token")
            return
        
        # Calculate buy volume
        results = analyzer.calculate_buy_volume(target_token)
        
        # Export results
        csv_file = analyzer.export_results(results)
        
        print("="*60)
        print("📊 FINAL SUMMARY")
        print("="*60)
        print(f"🛒 Total Historical Buy Volume: ${results['total_buy_volume_usd']:,.2f}")
        print(f"📈 Total Buy Transfers: {results['total_buy_transfers']:,}")
        print(f"👥 Unique Buyers: {results['unique_buyers']:,}")
        print(f"🕐 24h Buy Volume: ${results['h24_buy_volume_usd']:,.2f}")
        print(f"🔍 DexScreener 24h: ${results['dexscreener_24h_volume']:,.2f}")
        
        if results['dexscreener_24h_volume'] > 0:
            accuracy = (results['h24_buy_volume_usd'] / results['dexscreener_24h_volume']) * 100
            print(f"✅ Methodology Accuracy: {accuracy:.1f}%")
        
        print(f"📄 Detailed results: {csv_file}")
        print("🎉 Buy volume analysis complete!")
        
    except KeyboardInterrupt:
        print("\n❌ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 