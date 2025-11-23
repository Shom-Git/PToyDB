#!/usr/bin/env python3
"""
Example usage of DistDB - SQL Simulation.
Simulates a backend sending SQL queries to the database.
Runs until Ctrl+C is pressed.
"""

import time
import random
import signal
import sys
from distdb import Client

def signal_handler(sig, frame):
    print("\n\nReceived Ctrl+C! Stopping gracefully...")
    raise KeyboardInterrupt

def main():
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    print("=" * 60)
    print("DistDB - SQL Interface Simulation")
    print("Simulating backend sending SQL queries...")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    # Create a client (starts the database node)
    print("\n[SYSTEM] Starting database node...")
    client = Client()
    
    # Wait for Raft leader election
    print("[SYSTEM] Waiting for leader election (3s)...")
    time.sleep(3)
    
    try:
        # 1. DDL: Create Table
        print("\n[BACKEND] Sending DDL queries...")
        
        create_table_sql = """
        CREATE TABLE sensors (
            id TEXT, 
            timestamp INTEGER, 
            type TEXT, 
            value INTEGER
        )
        """
        print(f"SQL > {create_table_sql.strip().replace(chr(10), ' ')}")
        try:
            client.execute(create_table_sql)
            print("DB  < Success: Table 'sensors' created")
        except Exception as e:
            print(f"DB  < Note: {e}")

        create_index_sql = "CREATE INDEX idx_type ON sensors (type) USING BTREE"
        print(f"SQL > {create_index_sql}")
        try:
            client.execute(create_index_sql)
            print("DB  < Success: Index 'idx_type' created")
        except Exception as e:
            print(f"DB  < Note: {e}")

        print("\n[BACKEND] Starting SQL query stream...")
        print("-" * 60)
        
        counter = 0
        sensor_types = ['temperature', 'humidity', 'pressure', 'vibration']
        
        while True:
            counter += 1
            
            # Generate data
            sensor_id = f"sensor_{random.randint(1, 5)}"
            sensor_type = random.choice(sensor_types)
            value = random.randint(20, 100)
            ts = int(time.time())
            
            # 2. DML: Insert using SQL
            insert_sql = f"INSERT INTO sensors (id, timestamp, type, value) VALUES ('{sensor_id}', {ts}, '{sensor_type}', {value})"
            
            # Print the SQL being executed
            print(f"SQL > {insert_sql}")
            client.execute(insert_sql)
            
            # 3. DML: Select using SQL (every 5th op)
            if counter % 5 == 0:
                query_type = random.choice(sensor_types)
                select_sql = f"SELECT * FROM sensors WHERE type = '{query_type}'"
                
                print(f"SQL > {select_sql}")
                rows = client.query(select_sql)
                
                if rows:
                    avg_val = sum(r['value'] for r in rows) / len(rows)
                    print(f"DB  < Result: Found {len(rows)} rows, Avg Value: {avg_val:.1f}")
                else:
                    print(f"DB  < Result: No rows found")
            
            # Sleep to simulate real-time data
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("Simulation stopped by user.")
        
        # Final count using SQL
        try:
            count_result = client.query("SELECT * FROM sensors")
            print(f"Final DB State: {len(count_result)} total records")
        except:
            pass
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("Closing database...")
        client.close()
        print("✓ Database closed")
        sys.exit(0)

if __name__ == '__main__':
    main()
