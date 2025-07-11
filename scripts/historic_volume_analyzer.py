#!/usr/bin/env python3
"""
Historic Trading Volume Analyzer for Base Network Tokens
========================================================

Analyzes historic trading volume for multiple ERC-20 tokens on Base network.
Calculates both native token volume and USD volume using Basescan API.

Features:
- Multi-token analysis
- Trading volume only (excludes mint/burn transactions)
- USD conversion using current prices
- CSV export with detailed results
- Console summary with key metrics

Author: Token Analysis Tool
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
ETHERSCAN_API_KEY = "D69IA739C535GRHKHAYE6I1CFFESSRS8WR"  # Replace with your Etherscan API key
ETHERSCAN_V2_BASE_URL = "https://api.etherscan.io/v2/api"
BASE_CHAIN_ID = 8453  # Base Mainnet

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
    print("Historic Trading Volume Analyzer")
    print("=" * 50)
    print("🚀 Initializing Base network token analysis...")
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
        self.rate_limit_delay = 0.2  # 5 requests per second
        
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
    
    def get_token_info(self, contract_address: str) -> Dict:
        """Get token information using direct contract calls since tokeninfo requires API Pro"""
        """Get token information (name, symbol, decimals)"""
        # Get token supply to validate contract
        params = {
            'module': 'stats',
            'action': 'tokensupply',
            'contractaddress': contract_address
        }
        
        supply_data = self._make_request(params)
        if supply_data.get('status') != '1':
            return None
            
        # Note: Etherscan doesn't have a direct token info endpoint
        # We'll use a placeholder and update if we find the actual endpoint
        return {
            'address': contract_address,
            'name': f"Token_{contract_address[:8]}",
            'symbol': f"TKN_{contract_address[:6]}",
            'decimals': 18,  # Default, most tokens use 18
            'supply': supply_data.get('result', '0')
        }
    
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
            'sort': 'asc'
        }
        
        response = self._make_request(params)
        return response.get('result', [])
    
    def get_current_token_price_usd(self, contract_address: str) -> Optional[float]:
        """
        Get current token price in USD using Etherscan tokeninfo endpoint
        Falls back to DexScreener if Etherscan doesn't have price data
        """
        # Try Etherscan first (if they have a tokeninfo endpoint for Base)
        params = {
            'module': 'token',
            'action': 'tokeninfo',
            'contractaddress': contract_address
        }
        
        response = self._make_request(params)
        if response.get('status') == '1' and response.get('result'):
            result = response['result']
            if isinstance(result, list) and len(result) > 0:
                token_info = result[0]
                price_usd = token_info.get('tokenPriceUSD')
                if price_usd:
                    try:
                        return float(price_usd)
                    except (ValueError, TypeError):
                        pass
        
        # Fallback to DexScreener
        return self._get_price_from_dexscreener(contract_address)
    
    def _get_price_from_dexscreener(self, contract_address: str) -> Optional[float]:
        """Fallback price fetching from DexScreener"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get('pairs', [])
            
            # Find Base network pairs
            base_pairs = [p for p in pairs if p.get('chainId') == 'base']
            if not base_pairs:
                base_pairs = pairs  # Fallback to any pair
                
            if base_pairs:
                # Get the pair with highest liquidity
                best_pair = max(base_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                price_usd = best_pair.get('priceUsd')
                if price_usd:
                    return float(price_usd)
                    
        except Exception as e:
            print(f"⚠️  DexScreener price fetch failed for {contract_address[:10]}...: {e}")
            
        return None


class TokenConfig:
    """Configuration and metadata for a token"""
    
    def __init__(self, address: str, info: Dict = None):
        self.address = address.lower()
        self.info = info or {}
        self.name = self.info.get('name', f"Token_{address[:8]}")
        self.symbol = self.info.get('symbol', f"TKN_{address[:6]}")
        self.decimals = int(self.info.get('decimals', 18))
        self.supply = self.info.get('supply', '0')
        
    def format_amount(self, raw_amount: str) -> float:
        """Convert raw token amount to decimal format"""
        try:
            return float(raw_amount) / (10 ** self.decimals)
        except (ValueError, TypeError):
            return 0.0
    
    def __str__(self):
        return f"{self.name} ({self.symbol}) - {self.address[:10]}..."

class VolumeAnalyzer:
    """Main class for analyzing historic trading volume"""
    
    def __init__(self, api_key: str):
        self.api_client = BasescanAPIClient(api_key)
        self.tokens = {}
        self.volume_data = {}
        
    def setup_tokens(self, contract_addresses: List[str]) -> None:
        """Initialize token configurations"""
        print("🔧 Setting up token configurations...")
        
        for address in contract_addresses:
            print(f"  📋 Fetching info for {address[:10]}...")
            token_info = self.api_client.get_token_info(address)
            
            if token_info:
                self.tokens[address.lower()] = TokenConfig(address, token_info)
                print(f"    ✅ {self.tokens[address.lower()]}")
            else:
                print(f"    ⚠️  Could not fetch info, using defaults")
                self.tokens[address.lower()] = TokenConfig(address)
                
        print(f"✅ Configured {len(self.tokens)} tokens\n")
    
    def fetch_all_transfers(self, contract_address: str) -> List[Dict]:
        """Fetch all transfer events for a token"""
        token = self.tokens[contract_address.lower()]
        print(f"📥 Fetching transfers for {token.symbol}...")
        
        all_transfers = []
        page = 1
        max_pages = 100  # Safety limit
        
        with tqdm(desc=f"Loading {token.symbol} transfers", unit="page") as pbar:
            while page <= max_pages:
                transfers = self.api_client.get_token_transfers(
                    contract_address, page=page, offset=10000
                )
                
                if not transfers or len(transfers) == 0:
                    break
                    
                # Filter out mint/burn transactions (from/to zero address)
                trading_transfers = [
                    tx for tx in transfers 
                    if tx.get('from', '').lower() != ZERO_ADDRESS.lower() 
                    and tx.get('to', '').lower() != ZERO_ADDRESS.lower()
                ]
                
                all_transfers.extend(trading_transfers)
                pbar.update(1)
                pbar.set_postfix({
                    'Total': len(all_transfers),
                    'Page': page,
                    'Trading': len(trading_transfers),
                    'Raw': len(transfers)
                })
                
                # If we got less than the max per page, we're done
                if len(transfers) < 10000:
                    break
                    
                page += 1
        
        print(f"  ✅ Found {len(all_transfers):,} trading transfers\n")
        return all_transfers


    def calculate_volume(self, contract_address: str, transfers: List[Dict]) -> Dict:
        """Calculate trading volume metrics for a token"""
        token = self.tokens[contract_address.lower()]
        print(f"📊 Calculating volume for {token.symbol}...")
        
        if not transfers:
            return {
                'token': token,
                'total_transfers': 0,
                'total_volume_native': 0.0,
                'total_volume_usd': 0.0,
                'unique_addresses': 0,
                'first_transfer': None,
                'last_transfer': None,
                'daily_volume': {},
                'top_transfers': []
            }
        
        # Calculate basic metrics
        total_volume_native = 0.0
        daily_volume = defaultdict(float)
        unique_addresses = set()
        transfer_amounts = []
        
        for tx in transfers:
            # Convert amount to decimal format
            amount = token.format_amount(tx.get('value', '0'))
            total_volume_native += amount
            
            # Track unique addresses
            unique_addresses.add(tx.get('from', '').lower())
            unique_addresses.add(tx.get('to', '').lower())
            
            # Group by day
            timestamp = int(tx.get('timeStamp', 0))
            if timestamp:
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                daily_volume[date] += amount
            
            # Store transfer info for top transfers
            transfer_amounts.append({
                'amount': amount,
                'hash': tx.get('hash', ''),
                'from': tx.get('from', ''),
                'to': tx.get('to', ''),
                'timestamp': timestamp
            })
        
        # Sort transfers by amount to get top transfers
        top_transfers = sorted(transfer_amounts, key=lambda x: x['amount'], reverse=True)[:10]
        
        # Get price and calculate USD volume
        current_price_usd = self.api_client.get_current_token_price_usd(contract_address)
        total_volume_usd = total_volume_native * current_price_usd if current_price_usd else 0.0
        
        # Time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in transfers if tx.get('timeStamp')]
        first_transfer = min(timestamps) if timestamps else None
        last_transfer = max(timestamps) if timestamps else None
        
        results = {
            'token': token,
            'total_transfers': len(transfers),
            'total_volume_native': total_volume_native,
            'total_volume_usd': total_volume_usd,
            'current_price_usd': current_price_usd,
            'unique_addresses': len(unique_addresses),
            'first_transfer': datetime.fromtimestamp(first_transfer) if first_transfer else None,
            'last_transfer': datetime.fromtimestamp(last_transfer) if last_transfer else None,
            'daily_volume': dict(daily_volume),
            'top_transfers': top_transfers
        }
        
        print(f"  ✅ Volume: {total_volume_native:,.2f} {token.symbol}")
        if current_price_usd:
            print(f"  💰 USD Value: ${total_volume_usd:,.2f}")
        print(f"  📈 Transfers: {len(transfers):,}")
        print(f"  👥 Unique Addresses: {len(unique_addresses):,}\n")
        
        return results
    
    def analyze_all_tokens(self) -> Dict:
        """Analyze all configured tokens"""
        print("🚀 Starting comprehensive volume analysis...\n")
        
        results = {}
        summary = {
            'total_volume_usd': 0.0,
            'total_transfers': 0,
            'total_unique_addresses': set(),
            'tokens_analyzed': 0,
            'analysis_timestamp': datetime.now()
        }
        
        for address, token in self.tokens.items():
            try:
                # Fetch all transfers
                transfers = self.fetch_all_transfers(address)
                
                # Calculate volume
                volume_data = self.calculate_volume(address, transfers)
                results[address] = volume_data
                
                # Update summary
                summary['total_volume_usd'] += volume_data['total_volume_usd']
                summary['total_transfers'] += volume_data['total_transfers']
                summary['total_unique_addresses'].update([
                    tx['from'] for tx in transfers
                ] + [
                    tx['to'] for tx in transfers
                ])
                summary['tokens_analyzed'] += 1
                
            except Exception as e:
                print(f"❌ Error analyzing {token.symbol}: {e}")
                results[address] = {
                    'token': token,
                    'error': str(e),
                    'total_volume_usd': 0.0,
                    'total_transfers': 0
                }
        
        # Finalize summary
        summary['total_unique_addresses'] = len(summary['total_unique_addresses'])
        
        return {
            'results': results,
            'summary': summary
        }


    def export_to_csv(self, analysis_data: Dict, output_dir: str = "data/csv_outputs") -> str:
        """Export analysis results to CSV file"""
        print("📄 Exporting results to CSV...")
        
        # Prepare data for CSV export
        csv_data = []
        
        for address, data in analysis_data['results'].items():
            if 'error' in data:
                csv_data.append({
                    'Contract_Address': address,
                    'Token_Name': data['token'].name,
                    'Token_Symbol': data['token'].symbol,
                    'Decimals': data['token'].decimals,
                    'Total_Transfers': 0,
                    'Total_Volume_Native': 0.0,
                    'Total_Volume_USD': 0.0,
                    'Current_Price_USD': 0.0,
                    'Unique_Addresses': 0,
                    'First_Transfer': '',
                    'Last_Transfer': '',
                    'Analysis_Status': f"Error: {data['error']}"
                })
            else:
                csv_data.append({
                    'Contract_Address': address,
                    'Token_Name': data['token'].name,
                    'Token_Symbol': data['token'].symbol,
                    'Decimals': data['token'].decimals,
                    'Total_Transfers': data['total_transfers'],
                    'Total_Volume_Native': f"{data['total_volume_native']:.6f}",
                    'Total_Volume_USD': f"{data['total_volume_usd']:.2f}",
                    'Current_Price_USD': f"{data.get('current_price_usd', 0):.8f}",
                    'Unique_Addresses': data['unique_addresses'],
                    'First_Transfer': data['first_transfer'].strftime('%Y-%m-%d %H:%M:%S') if data['first_transfer'] else '',
                    'Last_Transfer': data['last_transfer'].strftime('%Y-%m-%d %H:%M:%S') if data['last_transfer'] else '',
                    'Analysis_Status': 'Success'
                })
        
        # Create DataFrame and export
        df = pd.DataFrame(csv_data)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/historic_volume_analysis_{timestamp}.csv"
        
        # Ensure directory exists
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        df.to_csv(filename, index=False)
        
        print(f"  ✅ Results exported to: {filename}")
        return filename
    
    def print_summary(self, analysis_data: Dict) -> None:
        """Print detailed console summary"""
        results = analysis_data['results']
        summary = analysis_data['summary']
        
        print("\n" + "="*80)
        print("📊 HISTORIC TRADING VOLUME ANALYSIS SUMMARY")
        print("="*80)
        
        print(f"🕐 Analysis Time: {summary['analysis_timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Network: Base Mainnet (Chain ID: {BASE_CHAIN_ID})")
        print(f"📋 Tokens Analyzed: {summary['tokens_analyzed']}")
        print()
        
        # Overall summary
        print("💰 OVERALL SUMMARY")
        print("-" * 40)
        print(f"Total Volume (USD): ${summary['total_volume_usd']:,.2f}")
        print(f"Total Transfers: {summary['total_transfers']:,}")
        print(f"Unique Addresses: {summary['total_unique_addresses']:,}")
        print()
        
        # Individual token results
        print("📈 TOKEN BREAKDOWN")
        print("-" * 80)
        
        for address, data in results.items():
            if 'error' in data:
                print(f"❌ {data['token'].symbol} ({address[:10]}...) - ERROR: {data['error']}")
                continue
                
            token = data['token']
            print(f"🪙 {token.symbol} - {token.name}")
            print(f"   Contract: {address}")
            print(f"   Volume: {data['total_volume_native']:,.6f} {token.symbol}")
            
            if data.get('current_price_usd'):
                print(f"   USD Value: ${data['total_volume_usd']:,.2f}")
                print(f"   Price: ${data['current_price_usd']:.8f}")
            else:
                print(f"   USD Value: Price data unavailable")
                
            print(f"   Transfers: {data['total_transfers']:,}")
            print(f"   Unique Addresses: {data['unique_addresses']:,}")
            
            if data['first_transfer'] and data['last_transfer']:
                print(f"   Period: {data['first_transfer'].strftime('%Y-%m-%d')} to {data['last_transfer'].strftime('%Y-%m-%d')}")
                
            print(f"   Top Transfer: {data['top_transfers'][0]['amount']:,.6f} {token.symbol}" if data['top_transfers'] else "   No transfers")
            print()
        
        print("="*80)
        print("✅ Analysis Complete!")
        print("="*80)

def main():
    """Main execution function"""
    print("Historic Trading Volume Analyzer for Base Network")
    print("=" * 60)
    print()
    
    # Check API key
    if ETHERSCAN_API_KEY == "YourApiKeyToken":
        print("⚠️  WARNING: Please update ETHERSCAN_API_KEY in the script!")
        print("   You can get a free API key from: https://etherscan.io/apis")
        print("   The script will still run but with rate limits.")
        print()
        
        user_input = input("Continue anyway? (y/N): ").strip().lower()
        if user_input != 'y':
            print("❌ Exiting. Please update the API key and try again.")
            return
        print()
    
    # Initialize analyzer
    analyzer = VolumeAnalyzer(ETHERSCAN_API_KEY)
    
    # Setup tokens
    analyzer.setup_tokens(TOKEN_CONTRACTS)
    
    # Run analysis
    try:
        analysis_data = analyzer.analyze_all_tokens()
        
        # Print results
        analyzer.print_summary(analysis_data)
        
        # Export to CSV
        csv_file = analyzer.export_to_csv(analysis_data)
        
        print(f"\n📄 Detailed results saved to: {csv_file}")
        print("🎉 Analysis complete!")
        
    except KeyboardInterrupt:
        print("\n❌ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
