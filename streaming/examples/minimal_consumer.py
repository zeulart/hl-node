#!/usr/bin/env python3
"""
Minimal Redis consumer - just reads and prints
"""

import redis
import json
import sys

def main():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    print("🚀 Listening to node_fills:ALL stream...")
    print("Press Ctrl+C to stop\n")
    
    try:
        # Read from stream starting from latest
        last_id = '$'
        
        while True:
            # Block and wait for new messages
            messages = r.xread({'node_fills:ALL': last_id}, block=1000, count=10)
            
            if messages:
                for stream_name, msgs in messages:
                    for msg_id, fields in msgs:
                        # Extract the data
                        raw_data = fields.get('data', '')
                        
                        try:
                            # Parse JSON
                            trade_data = json.loads(raw_data)
                            wallet = trade_data[0]
                            trade = trade_data[1]
                            
                            # Simple formatted output
                            print(f"💰 {trade['coin']:>6} | {trade['side']} | {trade['sz']:>8} @ {trade['px']:>10} | {wallet[:10]}...")
                            
                        except (json.JSONDecodeError, IndexError, KeyError):
                            print(f"❌ Parse error: {raw_data[:50]}...")
                        
                        last_id = msg_id
                        
    except KeyboardInterrupt:
        print("\n👋 Stopped")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()