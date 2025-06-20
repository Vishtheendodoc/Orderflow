import time
import json
import pandas as pd
from datetime import datetime
from collections import deque
import logging
from typing import Dict, List, Optional
from dhanhq import DhanContext, dhanhq

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderFlowAnalyzer:
    def __init__(self, client_id: str, access_token: str):
        """
        Initialize Dhan Order Flow Analyzer
        
        Args:
            client_id: Your Dhan client ID
            access_token: Your Dhan access token
        """
        context = DhanContext(client_id=client_id, access_token=access_token)
        self.dhan = dhanhq(context)

        self.order_flow_history = deque(maxlen=1000)  # Store last 1000 snapshots
        self.previous_book = None
        self.signals = []
        
    def get_market_depth(self, security_id: str, exchange_segment: str = "NSE_EQ") -> Optional[Dict]:
        try:
            # Convert security_id to integer for the API call
            security_id_int = int(security_id)
            securities = {exchange_segment: [security_id_int]}
            
            print(f"🔍 Requesting data: {securities}")  # Debug log
            
            response = self.dhan.quote_data(securities)
            print("📦 Raw response from Dhan:\n", json.dumps(response, indent=2))

            # Navigate the response structure - FIXED: Handle nested 'data' structure
            if "data" not in response:
                print(f"⚠️ No 'data' key in response")
                return None
                
            # The actual data is nested under response["data"]["data"]
            outer_data = response["data"]
            if "data" not in outer_data:
                print(f"⚠️ No nested 'data' key in response")
                return None
                
            nested_data = outer_data["data"]
            if exchange_segment not in nested_data:
                print(f"⚠️ Exchange segment '{exchange_segment}' not found in response")
                print(f"Available segments: {list(nested_data.keys())}")
                return None

            segment_data = nested_data[exchange_segment]
            
            # Try both string and integer keys for security_id
            security_data = None
            for key_format in [str(security_id), str(security_id_int), security_id]:
                if key_format in segment_data:
                    security_data = segment_data[key_format]
                    break
            
            if not security_data:
                print(f"⚠️ Security ID '{security_id}' not found in {exchange_segment}")
                print(f"Available security IDs: {list(segment_data.keys())}")
                return None
                
            print(f"✅ Successfully retrieved data for {security_id} on {exchange_segment}")
            return security_data
            
        except ValueError as e:
            logger.error(f"Invalid security_id format '{security_id}': {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching market depth for {security_id} on {exchange_segment}: {e}")
            return None


    def calculate_imbalance_ratio(self, market_depth: Dict) -> float:
        """
        Calculate bid-ask imbalance ratio
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Imbalance ratio (>1 = buying pressure, <1 = selling pressure)
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            
            if total_ask_qty == 0:
                return float('inf')
            
            return total_bid_qty / total_ask_qty
        except Exception as e:
            logger.error(f"Error calculating imbalance: {e}")
            return 1.0
    
    def calculate_weighted_prices(self, market_depth: Dict) -> Dict[str, float]:
        """
        Calculate volume-weighted bid and ask prices
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Dictionary with weighted bid and ask prices
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Calculate weighted bid price
            total_bid_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) 
                                for level in bid_levels)
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            
            # Calculate weighted ask price
            total_ask_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) 
                                for level in ask_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            
            weighted_bid = total_bid_value / total_bid_qty if total_bid_qty > 0 else 0
            weighted_ask = total_ask_value / total_ask_qty if total_ask_qty > 0 else 0
            
            return {
                'weighted_bid': weighted_bid,
                'weighted_ask': weighted_ask,
                'spread': weighted_ask - weighted_bid
            }
        except Exception as e:
            logger.error(f"Error calculating weighted prices: {e}")
            return {'weighted_bid': 0, 'weighted_ask': 0, 'spread': 0}
    
    def calculate_order_book_delta(self, current_book: Dict, previous_book: Dict) -> Dict:
        """
        Calculate order book changes between snapshots
        
        Args:
            current_book: Current market depth
            previous_book: Previous market depth
            
        Returns:
            Dictionary with bid/ask deltas
        """
        try:
            # Current totals
            current_bid_qty = sum(float(level.get('quantity', 0)) 
                                for level in current_book.get('depth', {}).get('buy', []))
            current_ask_qty = sum(float(level.get('quantity', 0)) 
                                for level in current_book.get('depth', {}).get('sell', []))
            
            # Previous totals
            previous_bid_qty = sum(float(level.get('quantity', 0)) 
                                 for level in previous_book.get('depth', {}).get('buy', []))
            previous_ask_qty = sum(float(level.get('quantity', 0)) 
                                 for level in previous_book.get('depth', {}).get('sell', []))
            
            bid_delta = current_bid_qty - previous_bid_qty
            ask_delta = current_ask_qty - previous_ask_qty
            net_flow = bid_delta - ask_delta
            
            return {
                'bid_delta': bid_delta,
                'ask_delta': ask_delta,
                'net_flow': net_flow
            }
        except Exception as e:
            logger.error(f"Error calculating delta: {e}")
            return {'bid_delta': 0, 'ask_delta': 0, 'net_flow': 0}
    
    def detect_large_orders(self, market_depth: Dict, threshold_multiplier: float = 2.0) -> Dict:
        """
        Detect unusually large orders in the book
        
        Args:
            market_depth: Market depth data
            threshold_multiplier: Multiplier for average size to detect large orders
            
        Returns:
            Dictionary with large order detection results
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Calculate average quantities
            bid_quantities = [float(level.get('quantity', 0)) for level in bid_levels]
            ask_quantities = [float(level.get('quantity', 0)) for level in ask_levels]
            
            avg_bid_qty = sum(bid_quantities) / len(bid_quantities) if bid_quantities else 0
            avg_ask_qty = sum(ask_quantities) / len(ask_quantities) if ask_quantities else 0
            
            # Detect large orders
            large_bids = [qty for qty in bid_quantities if qty > avg_bid_qty * threshold_multiplier]
            large_asks = [qty for qty in ask_quantities if qty > avg_ask_qty * threshold_multiplier]
            
            return {
                'large_bid_count': len(large_bids),
                'large_ask_count': len(large_asks),
                'max_bid_size': max(bid_quantities) if bid_quantities else 0,
                'max_ask_size': max(ask_quantities) if ask_quantities else 0,
                'avg_bid_size': avg_bid_qty,
                'avg_ask_size': avg_ask_qty
            }
        except Exception as e:
            logger.error(f"Error detecting large orders: {e}")
            return {'large_bid_count': 0, 'large_ask_count': 0, 'max_bid_size': 0, 'max_ask_size': 0}
    
    def analyze_depth_levels(self, market_depth: Dict) -> Dict:
        """
        Analyze order distribution across different depth levels
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Dictionary with depth analysis
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Top 5 levels vs deeper levels
            top5_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels[:5])
            deep_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels[5:])
            
            top5_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels[:5])
            deep_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels[5:])
            
            # Calculate ratios
            bid_depth_ratio = top5_bid_qty / deep_bid_qty if deep_bid_qty > 0 else float('inf')
            ask_depth_ratio = top5_ask_qty / deep_ask_qty if deep_ask_qty > 0 else float('inf')
            
            return {
                'bid_depth_ratio': bid_depth_ratio,
                'ask_depth_ratio': ask_depth_ratio,
                'top5_bid_qty': top5_bid_qty,
                'top5_ask_qty': top5_ask_qty,
                'deep_bid_qty': deep_bid_qty,
                'deep_ask_qty': deep_ask_qty
            }
        except Exception as e:
            logger.error(f"Error analyzing depth levels: {e}")
            return {'bid_depth_ratio': 0, 'ask_depth_ratio': 0}
    
    def generate_order_flow_signals(self, flow_data: Dict) -> str:
        """
        Generate trading signals based on order flow analysis
        
        Args:
            flow_data: Processed order flow data
            
        Returns:
            Signal string (BULLISH_FLOW, BEARISH_FLOW, NEUTRAL_FLOW)
        """
        try:
            imbalance = flow_data.get('imbalance_ratio', 1.0)
            net_flow = flow_data.get('net_flow', 0)
            large_bid_count = flow_data.get('large_orders', {}).get('large_bid_count', 0)
            large_ask_count = flow_data.get('large_orders', {}).get('large_ask_count', 0)
            
            # Signal generation logic
            bullish_signals = 0
            bearish_signals = 0
            
            # Imbalance-based signals
            if imbalance > 1.5:
                bullish_signals += 2
            elif imbalance < 0.67:
                bearish_signals += 2
            
            # Net flow signals
            if net_flow > 0:
                bullish_signals += 1
            elif net_flow < 0:
                bearish_signals += 1
            
            # Large order signals
            if large_bid_count > large_ask_count:
                bullish_signals += 1
            elif large_ask_count > large_bid_count:
                bearish_signals += 1
            
            # Generate final signal
            if bullish_signals > bearish_signals + 1:
                return "BULLISH_FLOW"
            elif bearish_signals > bullish_signals + 1:
                return "BEARISH_FLOW"
            else:
                return "NEUTRAL_FLOW"
                
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return "NEUTRAL_FLOW"
    
    def process_order_flow(self, security_id: str, exchange_segment: str = "NSE_EQ") -> Optional[Dict]:
        """
        Process complete order flow analysis for a security
        
        Args:
            security_id: Security identifier
            exchange_segment: Exchange segment
            
        Returns:
            Complete order flow analysis or None if error
        """
        try:
            # Get current market depth
            current_book = self.get_market_depth(security_id, exchange_segment)
            if not current_book:
                return None
            
            # Calculate basic metrics
            imbalance_ratio = self.calculate_imbalance_ratio(current_book)
            weighted_prices = self.calculate_weighted_prices(current_book)
            large_orders = self.detect_large_orders(current_book)
            depth_analysis = self.analyze_depth_levels(current_book)
            
            # Calculate deltas if we have previous data
            delta_data = {}
            if self.previous_book:
                delta_data = self.calculate_order_book_delta(current_book, self.previous_book)
            
            # Compile flow data
            flow_data = {
                'timestamp': datetime.now().isoformat(),
                'security_id': security_id,
                'ltp': current_book.get('ltp', 0),
                'imbalance_ratio': imbalance_ratio,
                'weighted_prices': weighted_prices,
                'large_orders': large_orders,
                'depth_analysis': depth_analysis,
                'net_flow': delta_data.get('net_flow', 0),
                'bid_delta': delta_data.get('bid_delta', 0),
                'ask_delta': delta_data.get('ask_delta', 0)
            }
            
            # Generate signal
            signal = self.generate_order_flow_signals(flow_data)
            flow_data['signal'] = signal
            
            # Store data
            self.order_flow_history.append(flow_data)
            self.previous_book = current_book
            
            return flow_data
            
        except Exception as e:
            logger.error(f"Error processing order flow: {e}")
            return None
    
    def run_continuous_monitoring(self, security_id: str, exchange_segment: str = "NSE_EQ", 
                                 interval: int = 1, duration: int = 3600):
        """
        Run continuous order flow monitoring
        
        Args:
            security_id: Security identifier
            exchange_segment: Exchange segment
            interval: Monitoring interval in seconds
            duration: Total monitoring duration in seconds
        """
        logger.info(f"Starting continuous monitoring for {security_id}")
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration:
                flow_data = self.process_order_flow(security_id, exchange_segment)
                
                if flow_data:
                    # Log key metrics
                    logger.info(f"Time: {flow_data['timestamp'][:19]} | "
                              f"LTP: {flow_data['ltp']:.2f} | "
                              f"Imbalance: {flow_data['imbalance_ratio']:.2f} | "
                              f"Net Flow: {flow_data['net_flow']:.0f} | "
                              f"Signal: {flow_data['signal']}")
                    
                    # Alert on strong signals
                    if flow_data['signal'] in ['BULLISH_FLOW', 'BEARISH_FLOW']:
                        logger.warning(f"ALERT: {flow_data['signal']} detected!")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")
    
    def get_flow_summary(self, lookback_minutes: int = 30) -> Dict:
        """
        Get summary of order flow over specified time period
        
        Args:
            lookback_minutes: Minutes to look back
            
        Returns:
            Summary statistics
        """
        try:
            cutoff_time = datetime.now().timestamp() - (lookback_minutes * 60)
            
            recent_data = [
                data for data in self.order_flow_history
                if datetime.fromisoformat(data['timestamp']).timestamp() > cutoff_time
            ]
            
            if not recent_data:
                return {}
            
            # Calculate summary statistics
            avg_imbalance = sum(d['imbalance_ratio'] for d in recent_data) / len(recent_data)
            total_net_flow = sum(d['net_flow'] for d in recent_data)
            
            signal_counts = {}
            for data in recent_data:
                signal = data['signal']
                signal_counts[signal] = signal_counts.get(signal, 0) + 1
            
            return {
                'period_minutes': lookback_minutes,
                'data_points': len(recent_data),
                'avg_imbalance_ratio': avg_imbalance,
                'total_net_flow': total_net_flow,
                'signal_distribution': signal_counts,
                'dominant_signal': max(signal_counts.items(), key=lambda x: x[1])[0]
            }
            
        except Exception as e:
            logger.error(f"Error generating flow summary: {e}")
            return {}
    
    def export_data_to_csv(self, filename: str = None):
        """
        Export order flow data to CSV
        
        Args:
            filename: Output filename (optional)
        """
        try:
            if not filename:
                filename = f"order_flow_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Convert to DataFrame
            df_data = []
            for data in self.order_flow_history:
                row = {
                    'timestamp': data['timestamp'],
                    'security_id': data['security_id'],
                    'ltp': data['ltp'],
                    'imbalance_ratio': data['imbalance_ratio'],
                    'net_flow': data['net_flow'],
                    'signal': data['signal'],
                    'weighted_bid': data['weighted_prices']['weighted_bid'],
                    'weighted_ask': data['weighted_prices']['weighted_ask'],
                    'spread': data['weighted_prices']['spread'],
                    'large_bid_count': data['large_orders']['large_bid_count'],
                    'large_ask_count': data['large_orders']['large_ask_count']
                }
                df_data.append(row)
            
            df = pd.DataFrame(df_data)
            df.to_csv(filename, index=False)
            logger.info(f"Data exported to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")

# 2. Update the main example to test NSE_FNO
if __name__ == "__main__":
    # Initialize analyzer
    CLIENT_ID = "1100244268"
    ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzUxMDA2OTE4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.caSAnGLGTZ0PSNcj0ICBfIQ9FgIxR68h8JHela-P151EQO9QucJ4KOfNEyGBwFtyEGPCBkBuQN2JyiYD0QzuSQ"
    
    analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
    
    # Test different exchanges
    test_securities = [
        ("53216", "NSE_FNO"),  # Nifty futures
        ("1333", "NSE_EQ"),    # HDFC Bank equity
    ]
    
    for security_id, exchange in test_securities:
        print(f"\n=== Testing {security_id} on {exchange} ===")
        result = analyzer.process_order_flow(security_id, exchange)
        if result:
            print(f"✅ Success: LTP={result.get('ltp')}, Signal={result.get('signal')}")
        else:
            print(f"❌ Failed to get data for {security_id} on {exchange}")