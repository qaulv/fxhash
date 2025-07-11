#!/usr/bin/env python3
"""
Historic Trading Volume Analyzer for Base Network Tokens (Updated)
==================================================================

Analyzes historic trading volume for multiple ERC-20 tokens on Base network.
Calculates both native token volume and USD volume using Basescan API.
Updated to fetch real token names and properly handle decimals.

Features:
- Multi-token analysis with real token names
- Proper decimals handling for accurate USD calculations
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
        """Check if a package is installed, install if not"""
        spec = importlib.util.find_spec(package_name)
        if spec is None:
        print(f"Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

# Auto-install required packages
required_packages = ['requests', 'pandas', 'tqdm', 'web3']
for package in required_packages:
        check_install_package(package)

from web3 import Web3

# Constants
BASE_CHAIN_ID = 8453
ETHERSCAN_V2_BASE_URL = "https://api.etherscan.io/v2/api"
BASE_RPC_URL = "https://mainnet.base.org"
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex/tokens"
ETHERSCAN_API_KEY = "D69IA739C535GRHKHAYE6I1CFFESSRS8WR"

# ERC20 ABI for basic token info
ERC20_ABI = [
        {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}
]

# Contract addresses to analyze
TOKEN_CONTRACTS = [
        "0x0E903C3BBf5ed7179B7D1Da391A3cEEa303134E0",
        "0xF9bc1E5BF79bBF09C940AFc063Ed563B0F4a3c95", 
        "0x43BbFF35b91e721C2359e9f837688cF83B6dBBF1",
        "0x499E5Db42DB458797A8afBaA3bB32B2C46A147fa"
]

class Web3TokenClient:
        """Client for fetching token information using Web3"""
    
    def __init__(self, rpc_url: str = BASE_RPC_URL):
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.web3.is_connected():
            raise Exception("Failed to connect to Base RPC")
        print(f"✅ Connected to Base network")
    
    def get_token_info(self, contract_address: str) -> Dict:
        """Get token name, symbol, decimals, and supply using direct contract calls"""
        try:
            contract = self.web3.eth.contract(
                address=Web3.to_checksum_address(contract_address),
                abi=ERC20_ABI
            )
            
            name = contract.functions.name().call()
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            total_supply = contract.functions.totalSupply().call()
            
            return {
                'name': name,
                'symbol': symbol, 
                'decimals': decimals,
                'supply': str(total_supply)
            }
        except Exception as e:
            print(f"⚠️  Failed to get token info for {contract_address}: {e}")
            return {
                'name': f"Token_{contract_address[:8]}",
                'symbol': f"TKN_{contract_address[:6]}",
                'decimals': 18,
                'supply': '0'
            }

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
                else:
                    print(f"⚠️  API Warning: {data.get('message', 'Unknown error')}")
                    if attempt == max_retries - 1:
                        return data
                        
            except Exception as e:
                print(f"⚠️  Request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
        
        return {}
    
    def get_token_transfers(self, contract_address: str, start_block: int = 0, end_block: int = 99999999) -> List[Dict]:
        """Fetch all ERC20 token transfer events"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': 'asc'
        }
        
        response = self._make_request(params)
        return response.get('result', [])

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
        self.web3_client = Web3TokenClient()
        self.tokens = {}
        
    def initialize_tokens(self, contract_addresses: List[str]) -> None:
        """Initialize token configurations with real metadata"""
        print("🔍 Fetching token information...")
        
        for address in contract_addresses:
            print(f"  📄 Getting info for {address[:10]}...")
            token_info = self.web3_client.get_token_info(address)
            self.tokens[address.lower()] = TokenConfig(address, token_info)
            print(f"    ✅ {token_info['name']} ({token_info['symbol']}) - {token_info['decimals']} decimals")
    
    def fetch_transfers(self, contract_address: str) -> List[Dict]:
        """Fetch all transfer events for a token"""
        token = self.tokens[contract_address.lower()]
        print(f"�� Fetching transfers for {token.name} ({token.symbol})...")
        
        transfers = self.api_client.get_token_transfers(contract_address)
        
        if not transfers:
            print(f"  ⚠️  No transfers found")
            return []
        
        # Filter out mint/burn transactions (from/to zero address)
        zero_address = "0x0000000000000000000000000000000000000000"
        trading_transfers = [
            tx for tx in transfers 
            if tx.get('from', '').lower() != zero_address.lower() 
            and tx.get('to', '').lower() != zero_address.lower()
        ]
        
        print(f"  ✅ Found {len(trading_transfers):,} trading transfers (filtered from {len(transfers):,} total)")
        return trading_transfers

    def get_current_price(self, contract_address: str) -> float:
        """Get current token price in USD"""
        try:
            # Try DexScreener API
            url = f"{DEXSCREENER_BASE_URL}/{contract_address}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data and 'pairs' in data and data['pairs']:
                # Get the pair with highest liquidity on Base
                base_pairs = [p for p in data['pairs'] if p.get('chainId') == 'base']
                if base_pairs:
                    best_pair = max(base_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                    price = float(best_pair.get('priceUsd', 0))
                    if price > 0:
                        return price
            
            print(f"  ⚠️  No price data available")
            return 0.0
            
        except Exception as e:
            print(f"  ⚠️  Price fetch failed: {e}")
            return 0.0

    def calculate_volume(self, contract_address: str, transfers: List[Dict]) -> Dict:
        """Calculate trading volume metrics for a token"""
        token = self.tokens[contract_address.lower()]
        print(f"📊 Calculating volume for {token.name} ({token.symbol})...")
        
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
            try:
                # Convert amount using proper decimals
                raw_amount = tx.get('value', '0')
                amount = token.format_amount(raw_amount)
                total_volume_native += amount
                
                # Daily volume tracking
                timestamp = int(tx.get('timeStamp', 0))
                if timestamp > 0:
                    date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    daily_volume[date] += amount
                
                # Track unique addresses
                unique_addresses.add(tx.get('from', '').lower())
                unique_addresses.add(tx.get('to', '').lower())
                
                # Store for top transfers
                transfer_amounts.append({
                    'amount': amount,
                    'hash': tx.get('hash', ''),
                    'timestamp': timestamp,
                    'from': tx.get('from', ''),
                    'to': tx.get('to', '')
                })
                
            except Exception as e:
                print(f"    ⚠️  Error processing transfer: {e}")
                continue
        
        # Get current price and calculate USD volume
        current_price = self.get_current_price(contract_address)
        total_volume_usd = total_volume_native * current_price
        
        # Get top 10 transfers
        top_transfers = sorted(transfer_amounts, key=lambda x: x['amount'], reverse=True)[:10]
        
        # Time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in transfers if tx.get('timeStamp')]
        first_transfer = datetime.fromtimestamp(min(timestamps)) if timestamps else None
        last_transfer = datetime.fromtimestamp(max(timestamps)) if timestamps else None
        
        return {
            'token': token,
            'total_transfers': len(transfers),
            'total_volume_native': total_volume_native,
            'total_volume_usd': total_volume_usd,
            'current_price_usd': current_price,
            'unique_addresses': len(unique_addresses),
            'first_transfer': first_transfer,
            'last_transfer': last_transfer,
            'daily_volume': dict(daily_volume),
            'top_transfers': top_transfers
        }

    def analyze_multiple_tokens(self, contract_addresses: List[str]) -> Dict:
        """Analyze trading volume for multiple tokens"""
        print(f"🚀 Starting analysis of {len(contract_addresses)} tokens...")
        print("=" * 60)
        
        # Initialize tokens with real metadata
        self.initialize_tokens(contract_addresses)
        print()
        
        results = {}
        total_usd_volume = 0.0
        total_transfers = 0
        total_unique_addresses = set()
        
        for address in contract_addresses:
            try:
                # Fetch transfers
                transfers = self.fetch_transfers(address)
                
                # Calculate volume metrics
                volume_data = self.calculate_volume_with_net_transfers(address, transfers)
                results[address] = volume_data
                
                # Aggregate totals
                total_usd_volume += volume_data['total_volume_usd']
                total_transfers += volume_data['total_transfers']
                
                print(f"  💰 Volume: ${volume_data['total_volume_usd']:,.2f} USD ({volume_data['total_volume_native']:,.2f} {volume_data['token'].symbol})")
                print(f"  📊 Price: ${volume_data['current_price_usd']:.6f}")
                print(f"  📈 Transfers: {volume_data['total_transfers']:,}")
                print(f"  👥 Unique addresses: {volume_data['unique_addresses']:,}")
                print()
                
            except Exception as e:
                print(f"❌ Error analyzing {address}: {e}")
                results[address] = {'error': str(e)}
                print()
        
        return {
            'results': results,
            'summary': {
                'total_usd_volume': total_usd_volume,
                'total_transfers': total_transfers,
                'tokens_analyzed': len([r for r in results.values() if 'error' not in r]),
                'analysis_date': datetime.now().isoformat()
            }
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
                    'Token_Name': f"Error_{address[:8]}",
                    'Token_Symbol': 'ERROR',
                    'Decimals': 18,
                    'Total_Transfers': 0,
                    'Total_Volume_Native': 0.0,
                    'Total_Volume_USD': 0.0,
                    'Current_Price_USD': 0.0,
                    'Unique_Addresses': 0,
                    'First_Transfer': '',
                    'Last_Transfer': '',
                    'Error': data['error']
                })
            else:
                csv_data.append({
                    'Contract_Address': address,
                    'Token_Name': data['token'].name,
                    'Token_Symbol': data['token'].symbol,
                    'Decimals': data['token'].decimals,
                    'Raw_Transfers': data['total_transfers'],
                    'Net_Transactions': data['net_transactions'],
                    'Raw_Volume_Native': round(data['raw_volume_native'], 6),
                    'Net_Volume_Native': round(data['net_volume_native'], 6),
                    'Raw_Volume_USD': round(data['raw_volume_usd'], 2),
                    'Net_Volume_USD': round(data['net_volume_usd'], 2),
                    'Volume_Reduction_Percent': round(data['volume_reduction_percent'], 1),
                    'Current_Price_USD': round(data['current_price_usd'], 8),
                    'Unique_Addresses': data['unique_addresses'],
                    'First_Transfer': data['first_transfer'].strftime('%Y-%m-%d %H:%M:%S') if data['first_transfer'] else '',
                    'Last_Transfer': data['last_transfer'].strftime('%Y-%m-%d %H:%M:%S') if data['last_transfer'] else '',
                    'Error': ''
                })
        
        # Create DataFrame and save
        df = pd.DataFrame(csv_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/historic_volume_analysis_{timestamp}.csv"
        df.to_csv(filename, index=False)
        
        print(f"✅ Results exported to: {filename}")
        return filename

    def print_summary(self, analysis_data: Dict) -> None:
        """Print detailed console summary"""
        print("=" * 80)
        print("📈 HISTORIC TRADING VOLUME ANALYSIS SUMMARY")
        print("=" * 80)
        
        summary = analysis_data['summary']
        print(f"💰 Total USD Volume: ${summary['total_usd_volume']:,.2f}")
        print(f"📊 Total Transfers: {summary['total_transfers']:,}")
        print(f"🪙  Tokens Analyzed: {summary['tokens_analyzed']}")
        print(f"📅 Analysis Date: {summary['analysis_date'][:19]}")
        print()
        
        # Sort tokens by USD volume
        successful_results = [(addr, data) for addr, data in analysis_data['results'].items() 
                             if 'error' not in data]
        successful_results.sort(key=lambda x: x[1]['total_volume_usd'], reverse=True)
        
        print("🏆 TOKEN RANKINGS BY VOLUME:")
        print("-" * 80)
        
        for i, (address, data) in enumerate(successful_results, 1):
            token = data['token']
            volume_usd = data['total_volume_usd']
            volume_native = data['total_volume_native']
            price = data['current_price_usd']
            transfers = data['total_transfers']
            unique_users = data['unique_addresses']
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i:2d}."
            
            print(f"{medal} {token.name} ({token.symbol})")
            print(f"    💰 Volume: ${volume_usd:,.2f} USD ({volume_native:,.2f} {token.symbol})")
            raw_volume_usd = data["raw_volume_usd"]
            raw_volume_native = data["raw_volume_native"]
            reduction = data["volume_reduction_percent"]
            print(f"    📊 Raw Volume: ${raw_volume_usd:,.2f} USD ({raw_volume_native:,.2f} {token.symbol}) | Reduction: {reduction:.1f}%")
            print(f"    💎 Price: ${price:.6f} USD")
            print(f"    📈 Net Transactions: {transfers:,}")
            raw_transfers = data["total_transfers"]
            print(f"    📈 Raw Transfers: {raw_transfers:,}")
            print(f"    👥 Unique Traders: {unique_users:,}")
            print(f"    📍 Contract: {address}")
            
            if data['first_transfer'] and data['last_transfer']:
                days_active = (data['last_transfer'] - data['first_transfer']).days
                print(f"    ⏰ Active Period: {days_active} days ({data['first_transfer'].strftime('%Y-%m-%d')} to {data['last_transfer'].strftime('%Y-%m-%d')})")
            
            print()


    def group_transfers_by_tx(self, transfers: List[Dict]) -> Dict[str, List[Dict]]:
        """Group transfers by transaction hash"""
        tx_groups = {}
        for transfer in transfers:
            tx_hash = transfer.get('hash', '')
            if tx_hash not in tx_groups:
                tx_groups[tx_hash] = []
            tx_groups[tx_hash].append(transfer)
        return tx_groups

    def calculate_net_transfers(self, transfers_in_tx: List[Dict], token: 'TokenConfig') -> float:
        """Calculate net economic transfer volume from a transaction's transfers"""
        if not transfers_in_tx:
            return 0.0
        
        # Track address flows: negative = outflow, positive = inflow
        address_flows = {}
        
        for transfer in transfers_in_tx:
            from_addr = transfer.get('from', '').lower()
            to_addr = transfer.get('to', '').lower()
            raw_amount = transfer.get('value', '0')
            amount = token.format_amount(raw_amount)
            
            # Skip zero amounts
            if amount == 0:
                continue
            
            # Track flows
            if from_addr not in address_flows:
                address_flows[from_addr] = 0.0
            if to_addr not in address_flows:
                address_flows[to_addr] = 0.0
                
            address_flows[from_addr] -= amount  # outflow
            address_flows[to_addr] += amount    # inflow
        
        # Calculate net volume: sum of absolute net flows / 2
        # (divide by 2 because each economic transfer has one sender and one receiver)
        net_flows = [abs(flow) for flow in address_flows.values() if abs(flow) > 0.001]
        net_volume = sum(net_flows) / 2 if net_flows else 0.0
        
        return net_volume

    def calculate_volume_with_net_transfers(self, contract_address: str, transfers: List[Dict]) -> Dict:
        """Calculate both raw and net trading volume metrics"""
        token = self.tokens[contract_address.lower()]
        print(f"📊 Calculating NET volume for {token.name} ({token.symbol})...")
        
        if not transfers:
            return self._empty_volume_result(token)
        
        # Group transfers by transaction hash
        tx_groups = self.group_transfers_by_tx(transfers)
        
        # Calculate raw volume (old method)
        raw_volume = sum(token.format_amount(tx.get('value', '0')) for tx in transfers)
        
        # Calculate net volume (new method)
        net_volume = 0.0
        net_transfers_count = 0
        daily_volume = {}
        unique_addresses = set()
        transfer_details = []
        
        for tx_hash, transfers_in_tx in tx_groups.items():
            # Calculate net volume for this transaction
            tx_net_volume = self.calculate_net_transfers(transfers_in_tx, token)
            net_volume += tx_net_volume
            
            if tx_net_volume > 0:
                net_transfers_count += 1
                
                # Use first transfer for metadata (timestamp, etc.)
                first_transfer = transfers_in_tx[0]
                timestamp = int(first_transfer.get('timeStamp', 0))
                
                # Daily volume tracking
                if timestamp > 0:
                    from datetime import datetime
                    date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    if date not in daily_volume:
                        daily_volume[date] = 0.0
                    daily_volume[date] += tx_net_volume
                
                # Track unique addresses from all transfers in tx
                for transfer in transfers_in_tx:
                    unique_addresses.add(transfer.get('from', '').lower())
                    unique_addresses.add(transfer.get('to', '').lower())
                
                # Store transaction details
                transfer_details.append({
                    'tx_hash': tx_hash,
                    'net_amount': tx_net_volume,
                    'raw_transfers': len(transfers_in_tx),
                    'timestamp': timestamp
                })
        
        # Get current price and calculate USD volumes
        current_price = self.get_current_price(contract_address)
        raw_volume_usd = raw_volume * current_price
        net_volume_usd = net_volume * current_price
        
        # Volume reduction percentage
        volume_reduction = ((raw_volume - net_volume) / raw_volume * 100) if raw_volume > 0 else 0
        
        # Top transfers (by net amount)
        top_transfers = sorted(transfer_details, key=lambda x: x['net_amount'], reverse=True)[:10]
        
        # Time range
        timestamps = [int(tx.get('timeStamp', 0)) for tx in transfers if tx.get('timeStamp')]
        first_transfer = datetime.fromtimestamp(min(timestamps)) if timestamps else None
        last_transfer = datetime.fromtimestamp(max(timestamps)) if timestamps else None
        
        print(f"  💰 Raw Volume: {raw_volume:,.2f} {token.symbol} (${raw_volume_usd:,.2f} USD)")
        print(f"  💎 Net Volume: {net_volume:,.2f} {token.symbol} (${net_volume_usd:,.2f} USD)")
        print(f"  📉 Volume Reduction: {volume_reduction:.1f}%")
        print(f"  📊 Net Transactions: {net_transfers_count:,} (from {len(transfers):,} raw transfers)")
        
        return {
            'token': token,
            'total_transfers': len(transfers),
            'net_transactions': net_transfers_count,
            'raw_volume_native': raw_volume,
            'net_volume_native': net_volume,
            'raw_volume_usd': raw_volume_usd,
            'net_volume_usd': net_volume_usd,
            'volume_reduction_percent': volume_reduction,
            'current_price_usd': current_price,
            'unique_addresses': len(unique_addresses),
            'first_transfer': first_transfer,
            'last_transfer': last_transfer,
            'daily_volume': daily_volume,
            'top_transfers': top_transfers
        }

    def _empty_volume_result(self, token):
        """Return empty volume result structure"""
        return {
            'token': token,
            'total_transfers': 0,
            'net_transactions': 0,
            'raw_volume_native': 0.0,
            'net_volume_native': 0.0,
            'raw_volume_usd': 0.0,
            'net_volume_usd': 0.0,
            'volume_reduction_percent': 0.0,
            'current_price_usd': 0.0,
            'unique_addresses': 0,
            'first_transfer': None,
            'last_transfer': None,
            'daily_volume': {},
            'top_transfers': []
        }
    def get_dexscreener_24h_volume(self, contract_address: str) -> Dict:
        """Get 24h volume from DexScreener for validation"""
        try:
            url = f"{DEXSCREENER_BASE_URL}/{contract_address}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data and 'pairs' in data and data['pairs']:
                # Get the pair with highest liquidity on Base
                base_pairs = [p for p in data['pairs'] if p.get('chainId') == 'base']
                if base_pairs:
                    best_pair = max(base_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                    volume_24h = float(best_pair.get('volume', {}).get('h24', 0))
                    price = float(best_pair.get('priceUsd', 0))
                    return {
                        'volume_24h_usd': volume_24h,
                        'price_usd': price,
                        'pair_name': best_pair.get('baseToken', {}).get('name', 'Unknown'),
                        'dex': best_pair.get('dexId', 'Unknown')
                    }
            
            return {'volume_24h_usd': 0, 'price_usd': 0, 'pair_name': 'Not Found', 'dex': 'None'}
            
        except Exception as e:
            print(f"  ⚠️  DexScreener API failed: {e}")
            return {'volume_24h_usd': 0, 'price_usd': 0, 'pair_name': 'Error', 'dex': 'Error'}

    def calculate_our_24h_volume(self, contract_address: str, transfers: List[Dict]) -> Tuple[float, int]:
        """Calculate our own 24h NET volume for comparison"""
        token = self.tokens[contract_address.lower()]
        
        # Get 24 hours ago timestamp
        twenty_four_hours_ago = int((datetime.now() - timedelta(hours=24)).timestamp())
        
        # Filter transfers to last 24h
        recent_transfers = [
            tx for tx in transfers 
            if int(tx.get('timeStamp', 0)) >= twenty_four_hours_ago
        ]
        
        if not recent_transfers:
            return 0.0, 0
        
        # Group by transaction hash
        tx_groups = self.group_transfers_by_tx(recent_transfers)
        
        # Calculate net volume
        net_volume_24h = 0.0
        net_transactions_24h = 0
        
        for tx_hash, transfers_in_tx in tx_groups.items():
            tx_net_volume = self.calculate_net_transfers(transfers_in_tx, token)
            if tx_net_volume > 0:
                net_volume_24h += tx_net_volume
                net_transactions_24h += 1
        
        return net_volume_24h, net_transactions_24h

    def validate_with_dexscreener(self, analysis_data: Dict) -> Dict:
        """Validate our volume calculations against DexScreener 24h data"""
        print("\n🔍 VALIDATION: Comparing with DexScreener 24h volumes...")
        print("=" * 80)
        
        validation_results = {}
        
        for address, data in analysis_data['results'].items():
            if 'error' in data:
                continue
                
            token = data['token']
            print(f"\n📊 Validating {token.name} ({token.symbol})...")
            
            # Get DexScreener 24h data
            dex_data = self.get_dexscreener_24h_volume(address)
            
            # Calculate our 24h volume
            # We need to fetch transfers again for 24h calculation
            transfers = self.fetch_transfers(address)
            our_volume_24h_native, our_transfers_24h = self.calculate_our_24h_volume(address, transfers)
            our_volume_24h_usd = our_volume_24h_native * data['current_price_usd']
            
            # Compare results
            dex_volume_24h = dex_data['volume_24h_usd']
            difference_usd = abs(our_volume_24h_usd - dex_volume_24h)
            difference_percent = (difference_usd / max(dex_volume_24h, 1)) * 100 if dex_volume_24h > 0 else 0
            
            validation_results[address] = {
                'token': token,
                'our_24h_volume_native': our_volume_24h_native,
                'our_24h_volume_usd': our_volume_24h_usd,
                'our_24h_transfers': our_transfers_24h,
                'dex_24h_volume_usd': dex_volume_24h,
                'difference_usd': difference_usd,
                'difference_percent': difference_percent,
                'dex_data': dex_data
            }
            
            # Print comparison
            print(f"  🔹 Our 24h Volume: ${our_volume_24h_usd:,.2f} USD ({our_volume_24h_native:,.2f} {token.symbol})")
            print(f"  🔹 DexScreener 24h: ${dex_volume_24h:,.2f} USD")
            print(f"  🔹 Difference: ${difference_usd:,.2f} USD ({difference_percent:.1f}%)")
            print(f"  🔹 Our 24h Transfers: {our_transfers_24h:,}")
            print(f"  🔹 DEX: {dex_data['dex']} | Pair: {dex_data['pair_name']}")
            
            # Status assessment
            if dex_volume_24h == 0:
                status = "⚪ No DexScreener data"
            elif difference_percent < 10:
                status = "✅ Very close match"
            elif difference_percent < 25:
                status = "🟡 Reasonable match"
            elif difference_percent < 50:
                status = "🟠 Some difference"
            else:
                status = "🔴 Significant difference"
            
            print(f"  📊 Status: {status}")
        
        return validation_results

    def print_validation_summary(self, validation_results: Dict) -> None:
        """Print validation summary"""
        print("\n" + "=" * 80)
        print("🎯 VALIDATION SUMMARY")
        print("=" * 80)
        
        total_our_24h = sum(r['our_24h_volume_usd'] for r in validation_results.values())
        total_dex_24h = sum(r['dex_24h_volume_usd'] for r in validation_results.values())
        
        print(f"💰 Our Total 24h Volume: ${total_our_24h:,.2f} USD")
        print(f"💰 DexScreener Total 24h: ${total_dex_24h:,.2f} USD")
        
        if total_dex_24h > 0:
            overall_difference = abs(total_our_24h - total_dex_24h)
            overall_percent = (overall_difference / total_dex_24h) * 100
            print(f"📊 Overall Difference: ${overall_difference:,.2f} USD ({overall_percent:.1f}%)")
        
        print("\n🔍 Individual Token Validation:")
        print("-" * 50)
        
        for address, result in validation_results.items():
            token = result['token']
            status_emoji = "✅" if result['difference_percent'] < 10 else "🟡" if result['difference_percent'] < 25 else "🔴"
            print(f"{status_emoji} {token.name} ({token.symbol}): {result['difference_percent']:.1f}% difference")
        
        print("\n📝 Notes:")
        print("• Differences can occur due to:")
        print("  - Different time windows (DexScreener vs our 24h calculation)")
        print("  - DEX-only vs all transfer events")
        print("  - Price calculation timing differences")
        print("  - Data source variations")

    def main():
        """Main execution function with DexScreener validation"""
        print("🎯 Historic Trading Volume Analyzer")
        print("📊 Base Network Token Analysis with DexScreener Validation")
        print("=" * 60)
    
        # Initialize analyzer
        analyzer = VolumeAnalyzer(ETHERSCAN_API_KEY)
    
        # Run analysis
        analysis_data = analyzer.analyze_multiple_tokens(TOKEN_CONTRACTS)
    
        # Print summary
        analyzer.print_summary(analysis_data)
    
        # Validate with DexScreener
        validation_results = analyzer.validate_with_dexscreener(analysis_data)
        analyzer.print_validation_summary(validation_results)
    
        # Export to CSV (including validation data)
        csv_file = analyzer.export_to_csv(analysis_data)
    
        print("\n✅ Analysis complete!")
        print(f"📄 Results saved to: {csv_file}")
        print("🔍 Validation completed - check results above for accuracy assessment")

if __name__ == "__main__":
        main()
