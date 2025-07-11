
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

