#!/usr/bin/env python3
"""
Buy + Sell Volume Analyzer for Base Network Tokens
=================================================

CORRECTLY calculates trading volume by counting BOTH:
1. BUY volume (tokens FROM pool TO users)
2. SELL volume (tokens FROM users TO pool)
3. Total volume = BUY + SELL (matches DexScreener methodology)

This addresses the discrepancy where:
- DexScreener 24h: $4,276.86 
- Our buy-only: $1,927.95 (45% of total)
- Expected: buy + sell = $4,276.86

Author: Token Analysis Tool - Buy + Sell Volume
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
    print("📊 Buy + Sell Volume Analyzer")
    print("=" * 50)
    print("🎯 Counting BOTH buys and sells to match DexScreener")
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
                'all_pools': [p.get('pairAddress', '').lower() for p in base_pairs if p.get('pairAddress')],
                'price_change_24h': main_pair.get('priceChange', {}).get('h24', 0)
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
    
    def get_recent_transfers(self, contract_address: str, hours: int = 24) -> List[Dict]:
        """Get transfers from the last N hours"""
        print(f"📥 Fetching {hours}h transfers for {contract_address[:10]}...")
        
        cutoff_time = int(time.time()) - (hours * 3600)
        all_transfers = []
        page = 1
        max_pages = 50
        
        with tqdm(desc=f"Loading {hours}h transfers", unit="page") as pbar:
            while page <= max_pages:
                params = {
                    'module': 'account',
                    'action': 'tokentx',
                    'contractaddress': contract_address,
                    'startblock': 0,
                    'endblock': 99999999,
                    'page': page,
                    'offset': 10000,
                    'sort': 'desc'  # Get newest first
                }
                
                response = self._make_request(params)
                transfers = response.get('result', [])
                
                if not transfers:
                    break
                
                # Filter by time period and exclude mint/burn
                recent_transfers = []
                page_has_recent = False
                
                for tx in transfers:
                    timestamp = int(tx.get('timeStamp', 0))
                    
                    # Check if this transfer is within our time window
                    if timestamp >= cutoff_time:
                        page_has_recent = True
                        # Exclude mint/burn transactions
                        if (tx.get('from', '').lower() != ZERO_ADDRESS.lower() and 
                            tx.get('to', '').lower() != ZERO_ADDRESS.lower()):
                            recent_transfers.append(tx)
                
                # If no transfers on this page are recent, we can stop
                if not page_has_recent:
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
        
        print(f"  ✅ Found {len(all_transfers):,} transfers in {hours}h\n")
        return all_transfers
    
    def get_all_transfers(self, contract_address: str) -> List[Dict]:
        """Get ALL transfers (complete history)"""
        print(f"📥 Fetching ALL transfers for {contract_address[:10]}...")
        
        all_transfers = []
        page = 1
        max_pages = 200
        
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
                    'sort': 'asc'
                }
                
                response = self._make_request(params)
                transfers = response.get('result', [])
                
                if not transfers:
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
                    'Valid': len(valid_transfers)
                })
                
                if len(transfers) < 10000:
                    break
                    
                page += 1
        
        print(f"  ✅ Found {len(all_transfers):,} total transfers\n")
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
        self.dex_txns_24h = dex_info.get('txns_24h', {})
        
    def format_amount(self, raw_amount: str) -> float:
        """Convert raw token amount to decimal format"""
        try:
            return float(raw_amount) / (10 ** self.decimals)
        except (ValueError, TypeError):
            return 0.0
    
    def is_pool_address(self, address: str) -> bool:
        """Check if an address is a known DEX pool"""
        return address.lower() in self.pool_addresses

class BuySellVolumeAnalyzer:
    """Analyzer that counts both BUY and SELL volume to match DexScreener"""
    
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
        
        print(f"  ✅ {token.symbol} - {token.name} (${token.price_usd:.6f})")
        print(f"  🏊 Main Pool: {token.main_pool}")
        print(f"  🏊 All Pools: {len(token.pool_addresses)} pools")
        print(f"  📊 DexScreener 24h Volume: ${token.dex_volume_24h:,.2f}")
        print(f"  📊 DexScreener 24h Txns: {token.dex_txns_24h}")
        print()
        
        return True
    
    def analyze_trading_volume(self, contract_address: str) -> Dict:
        """Analyze both buy and sell volume for validation"""
        token = self.tokens[contract_address.lower()]
        print(f"📊 Analyzing trading volume for {token.symbol}...")
        
        # Get both 24h and all-time transfers
        transfers_24h = self.etherscan_client.get_recent_transfers(contract_address, 24)
        transfers_all = self.etherscan_client.get_all_transfers(contract_address)
        
        # Analyze both periods
        results_24h = self._analyze_period(token, transfers_24h, "24h")
        results_all = self._analyze_period(token, transfers_all, "all_time")
        
        # Combine results
        return {
            'contract_address': contract_address,
            'token': token,
            'periods': {
                '24h': results_24h,
                'all_time': results_all
            },
            'validation': {
                'dexscreener_24h_volume': token.dex_volume_24h,
                'dexscreener_24h_txns': token.dex_txns_24h,
                'our_24h_total': results_24h['total_volume_usd'],
                'our_24h_txns': results_24h['total_transactions'],
                'accuracy_volume': (results_24h['total_volume_usd'] / token.dex_volume_24h * 100) if token.dex_volume_24h > 0 else 0,
                'accuracy_txns': (results_24h['total_transactions'] / sum(token.dex_txns_24h.values()) * 100) if sum(token.dex_txns_24h.values()) > 0 else 0
            }
        }
    
    def _analyze_period(self, token: TokenConfig, transfers: List[Dict], period_name: str) -> Dict:
        """Analyze transfers for a specific period"""
        if not transfers:
            return self._empty_period_result(period_name)
        
        # Separate transfers by type
        buy_transfers = []
        sell_transfers = []
        other_transfers = []
        
        for transfer in transfers:
            from_addr = transfer.get('from', '').lower()
            to_addr = transfer.get('to', '').lower()
            
            if token.is_pool_address(from_addr):
                # Tokens flowing FROM pool TO user = BUY
                buy_transfers.append(transfer)
            elif token.is_pool_address(to_addr):
                # Tokens flowing FROM user TO pool = SELL
                sell_transfers.append(transfer)
            else:
                # Other transfers (user to user, etc.)
                other_transfers.append(transfer)
        
        # Calculate volumes
        buy_volume_native = sum(token.format_amount(tx.get('value', '0')) for tx in buy_transfers)
        sell_volume_native = sum(token.format_amount(tx.get('value', '0')) for tx in sell_transfers)
        
        buy_volume_usd = buy_volume_native * token.price_usd
        sell_volume_usd = sell_volume_native * token.price_usd
        total_volume_usd = buy_volume_usd + sell_volume_usd
        
        # Time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in transfers if tx.get('timeStamp')]
        first_transfer = min(timestamps) if timestamps else None
        last_transfer = max(timestamps) if timestamps else None
        
        # Unique addresses
        unique_buyers = len(set(tx.get('to', '') for tx in buy_transfers))
        unique_sellers = len(set(tx.get('from', '') for tx in sell_transfers))
        
        return {
            'period': period_name,
            'total_transfers': len(transfers),
            'total_transactions': len(buy_transfers) + len(sell_transfers),
            'total_volume_usd': total_volume_usd,
            'total_volume_native': buy_volume_native + sell_volume_native,
            
            'buy_volume_usd': buy_volume_usd,
            'buy_volume_native': buy_volume_native,
            'buy_transfers': len(buy_transfers),
            'unique_buyers': unique_buyers,
            
            'sell_volume_usd': sell_volume_usd,
            'sell_volume_native': sell_volume_native,
            'sell_transfers': len(sell_transfers),
            'unique_sellers': unique_sellers,
            
            'other_transfers': len(other_transfers),
            'first_transfer': datetime.fromtimestamp(first_transfer) if first_transfer else None,
            'last_transfer': datetime.fromtimestamp(last_transfer) if last_transfer else None
        }
    
    def _empty_period_result(self, period_name: str) -> Dict:
        """Return empty result for a period"""
        return {
            'period': period_name,
            'total_volume_usd': 0.0,
            'total_transactions': 0,
            'error': 'No transfers found'
        }
    
    def print_analysis(self, results: Dict) -> None:
        """Print detailed analysis results"""
        token = results['token']
        validation = results['validation']
        
        print("=" * 80)
        print(f"📊 TRADING VOLUME ANALYSIS: {token.symbol}")
        print("=" * 80)
        
        for period_name, period_data in results['periods'].items():
            if 'error' in period_data:
                print(f"❌ {period_name}: {period_data['error']}")
                continue
                
            print(f"\n📅 {period_name.upper()} PERIOD")
            print("-" * 40)
            print(f"💰 Total Volume: ${period_data['total_volume_usd']:,.2f}")
            print(f"  🛒 Buy Volume: ${period_data['buy_volume_usd']:,.2f} ({period_data['buy_transfers']} txns)")
            print(f"  🔻 Sell Volume: ${period_data['sell_volume_usd']:,.2f} ({period_data['sell_transfers']} txns)")
            print(f"📊 Total Transactions: {period_data['total_transactions']:,}")
            print(f"👥 Unique Buyers: {period_data['unique_buyers']:,}")
            print(f"👥 Unique Sellers: {period_data['unique_sellers']:,}")
            
            # Calculate ratios
            if period_data['total_volume_usd'] > 0:
                buy_ratio = period_data['buy_volume_usd'] / period_data['total_volume_usd'] * 100
                sell_ratio = period_data['sell_volume_usd'] / period_data['total_volume_usd'] * 100
                print(f"📈 Buy/Sell Ratio: {buy_ratio:.1f}% buys, {sell_ratio:.1f}% sells")
        
        # Validation section
        print(f"\n🔍 VALIDATION vs DEXSCREENER")
        print("-" * 40)
        print(f"DexScreener 24h Volume: ${validation['dexscreener_24h_volume']:,.2f}")
        print(f"Our 24h Volume: ${validation['our_24h_total']:,.2f}")
        print(f"Volume Accuracy: {validation['accuracy_volume']:.1f}%")
        print()
        print(f"DexScreener 24h Txns: {sum(validation['dexscreener_24h_txns'].values()) if validation['dexscreener_24h_txns'] else 0}")
        print(f"Our 24h Txns: {validation['our_24h_txns']:,}")
        print(f"Transaction Accuracy: {validation['accuracy_txns']:.1f}%")
        
        # Assessment
        volume_accuracy = validation['accuracy_volume']
        if 90 <= volume_accuracy <= 110:
            print("✅ EXCELLENT: Volume calculation matches DexScreener!")
        elif 70 <= volume_accuracy <= 130:
            print("🟡 GOOD: Volume calculation is close to DexScreener")
        else:
            print("⚠️ NEEDS WORK: Volume calculation differs significantly from DexScreener")
        
        print()
    
    def export_results(self, results: Dict, output_dir: str = "data/csv_outputs") -> str:
        """Export results to CSV"""
        print("📄 Exporting trading volume analysis...")
        
        # Prepare CSV data
        csv_data = []
        
        for period_name, period_data in results['periods'].items():
            if 'error' in period_data:
                continue
                
            csv_data.append({
                'Contract_Address': results['contract_address'],
                'Token_Symbol': results['token'].symbol,
                'Token_Name': results['token'].name,
                'Period': period_name,
                'Current_Price_USD': f"{results['token'].price_usd:.8f}",
                'Total_Volume_USD': f"{period_data['total_volume_usd']:.2f}",
                'Buy_Volume_USD': f"{period_data['buy_volume_usd']:.2f}",
                'Sell_Volume_USD': f"{period_data['sell_volume_usd']:.2f}",
                'Total_Transactions': period_data['total_transactions'],
                'Buy_Transactions': period_data['buy_transfers'],
                'Sell_Transactions': period_data['sell_transfers'],
                'Unique_Buyers': period_data['unique_buyers'],
                'Unique_Sellers': period_data['unique_sellers'],
                'Other_Transfers': period_data['other_transfers'],
                'DexScreener_24h_Volume': f"{results['validation']['dexscreener_24h_volume']:.2f}" if period_name == '24h' else '',
                'Volume_Accuracy_Pct': f"{results['validation']['accuracy_volume']:.1f}" if period_name == '24h' else '',
                'Analysis_Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(csv_data)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/trading_volume_analysis_{timestamp}.csv"
        
        # Ensure directory exists
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        df.to_csv(filename, index=False)
        
        print(f"  ✅ Results exported to: {filename}")
        return filename

def main():
    """Main execution function"""
    print("📊 Buy + Sell Volume Analyzer")
    print("=" * 60)
    print("🎯 Goal: Match DexScreener volume by counting both buys and sells")
    print("🔍 Validate our methodology accuracy")
    print()
    
    # Initialize analyzer
    analyzer = BuySellVolumeAnalyzer(ETHERSCAN_API_KEY)
    
    # Focus on GALO token for validation
    target_token = "0xEA3B0233d176D03d1484d75f46fE0fA611F4B07c"
    
    print("="*60)
    print(f"🎯 VALIDATION ANALYSIS: GALO Token")
    print("="*60)
    
    try:
        # Setup token
        if not analyzer.setup_token(target_token):
            print("❌ Failed to setup token")
            return
        
        # Analyze trading volume
        results = analyzer.analyze_trading_volume(target_token)
        
        # Print results
        analyzer.print_analysis(results)
        
        # Export results
        csv_file = analyzer.export_results(results)
        
        print("="*60)
        print("🎯 VALIDATION SUMMARY")
        print("="*60)
        validation = results['validation']
        print(f"🔍 DexScreener 24h: ${validation['dexscreener_24h_volume']:,.2f}")
        print(f"📊 Our 24h Total: ${validation['our_24h_total']:,.2f}")
        print(f"✅ Accuracy: {validation['accuracy_volume']:.1f}%")
        print(f"📄 Detailed results: {csv_file}")
        
        if validation['accuracy_volume'] > 90:
            print("🎉 SUCCESS: Our methodology matches DexScreener!")
        else:
            print("🔧 NEEDS REFINEMENT: Further analysis required")
        
    except KeyboardInterrupt:
        print("\n❌ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 