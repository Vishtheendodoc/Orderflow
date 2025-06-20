from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import json
import threading
import time
from datetime import datetime
from orderflow import OrderFlowAnalyzer  # Import your existing class
import csv
from collections import defaultdict, deque



app = Flask(__name__)
CORS(app)

# Global variables
analyzer = None
current_data = {}
monitoring_active = False
monitoring_thread = None

delta_history = defaultdict(lambda: deque(maxlen=60))  # Keep last 60 minutes per stock

@app.route("/api/stocks", methods=["GET"])
def get_stock_list():
    stock_file = "stock_list.csv"  # Ensure path is correct
    stocks = []
    try:
        with open(stock_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                stocks.append({
                    "security_id": row["security_id"],
                    "symbol": row["symbol"]
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify(stocks)


@app.route('/')
def dashboard():
    return render_template('dashboard.html')  # Your HTML file

@app.route('/api/start_monitoring', methods=['POST'])
def start_monitoring():
    global analyzer, monitoring_active, monitoring_thread
    
    data = request.get_json()
    security_id = data.get('security_id', '53216')
    
    # Fix: Map frontend exchange values to correct Dhan API segments
    exchange_mapping = {
        'NSE_EQ': 'NSE_EQ',
        'NSE_FO': 'NSE_FNO',  # Change: NSE_FO -> NSE_FNO
        'BSE_EQ': 'BSE_EQ'
    }
    
    frontend_exchange = data.get('exchange', 'NSE_FO')
    exchange = exchange_mapping.get(frontend_exchange, 'NSE_FNO')  # Default to NSE_FNO
    
    interval = data.get('interval', 2)
    
    print(f"Starting monitoring for Security ID: {security_id}, Exchange: {exchange}")  # Debug log
    
    # Initialize analyzer if not done
    if not analyzer:
        CLIENT_ID = "1100244268"
        ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzUxMDA2OTE4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.caSAnGLGTZ0PSNcj0ICBfIQ9FgIxR68h8JHela-P151EQO9QucJ4KOfNEyGBwFtyEGPCBkBuQN2JyiYD0QzuSQ"
        analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
    
    # Start monitoring in background thread
    if not monitoring_active:
        monitoring_active = True
        monitoring_thread = threading.Thread(
            target=continuous_monitor, 
            args=(security_id, exchange, interval)
        )
        monitoring_thread.daemon = True
        monitoring_thread.start()
    
    return jsonify({'status': 'started', 'message': f'Monitoring started for {exchange}'})
    
    # 2. Add better error handling in continuous_monitor
def continuous_monitor(security_id, exchange, interval):
    global current_data, monitoring_active, analyzer
    
    print(f"Starting continuous monitoring: Security={security_id}, Exchange={exchange}")
    
    while monitoring_active:
        try:
            # Get order flow data from your analyzer
            flow_data = analyzer.process_order_flow(security_id, exchange)
            
            if flow_data:
                # Transform data for dashboard
                dashboard_data = transform_for_dashboard(flow_data)
                current_data = dashboard_data
                delta_point = {
                    'timestamp': dashboard_data['timestamp'],
                    'bid_delta': flow_data.get('bid_delta', 0),
                    'ask_delta': flow_data.get('ask_delta', 0),
                    'net_flow': flow_data.get('net_flow', 0)
                }
                delta_history[security_id].append(delta_point)
                print(f"✅ Updated data: {datetime.now()}, Signal: {flow_data.get('signal', 'UNKNOWN')}")
            else:
                print(f"⚠️  No flow data received for {security_id} on {exchange}")
            
        except Exception as e:
            print(f"❌ Error in monitoring {security_id}/{exchange}: {e}")
        
        time.sleep(interval)

@app.route('/api/stop_monitoring', methods=['POST'])
def stop_monitoring():
    global monitoring_active
    monitoring_active = False
    return jsonify({'status': 'stopped', 'message': 'Monitoring stopped'})

@app.route('/api/current_data')
def get_current_data():
    print("Current Data Snapshot:", current_data)  # Add this
    return jsonify(current_data)

def continuous_monitor(security_id, exchange, interval):
    global current_data, monitoring_active, analyzer
    
    while monitoring_active:
        try:
            # Get order flow data from your analyzer
            flow_data = analyzer.process_order_flow(security_id, exchange)
            
            if flow_data:
                # Transform data for dashboard
                dashboard_data = transform_for_dashboard(flow_data)
                current_data = dashboard_data
                delta_point = {
                    'timestamp': dashboard_data['timestamp'],
                    'bid_delta': flow_data.get('bid_delta', 0),
                    'ask_delta': flow_data.get('ask_delta', 0),
                    'net_flow': flow_data.get('net_flow', 0)
                }
                delta_history[security_id].append(delta_point)

                print(f"Updated data: {datetime.now()}")  # Debug log
            
        except Exception as e:
            print(f"Error in monitoring: {e}")
        
        time.sleep(interval)

@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    history = list(delta_history.get(security_id, []))
    return jsonify(history)


def transform_for_dashboard(flow_data):
    """Transform analyzer data to dashboard format"""
    try:
        # Get order book from the latest market depth
        market_depth = analyzer.previous_book if analyzer.previous_book else {}
        
        # Extract order book levels
        bid_levels = market_depth.get('depth', {}).get('buy', [])[:10]
        ask_levels = market_depth.get('depth', {}).get('sell', [])[:10]
        
        # Format for dashboard
        order_book = {
            'bids': [{'price': level.get('price', 0), 'quantity': level.get('quantity', 0)} 
                     for level in bid_levels],
            'asks': [{'price': level.get('price', 0), 'quantity': level.get('quantity', 0)} 
                     for level in ask_levels]
        }
        
        # Large orders detection
        large_orders = flow_data.get('large_orders', {})
        
        return {
            'ltp': flow_data.get('ltp', 0),
            'imbalanceRatio': flow_data.get('imbalance_ratio', 1.0),
            'netFlow': flow_data.get('net_flow', 0),
            'spread': flow_data.get('weighted_prices', {}).get('spread', 0),
            'signal': flow_data.get('signal', 'NEUTRAL_FLOW'),
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'orderBook': order_book,
            'largeOrders': {
                'largeBids': large_orders.get('large_bid_count', 0),
                'largeAsks': large_orders.get('large_ask_count', 0)
            }
        }
    except Exception as e:
        print(f"Error transforming data: {e}")
        return {}

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)