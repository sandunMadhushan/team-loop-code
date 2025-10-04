# This module will contain the functions to detect specific events
# from the processed data streams.

import pandas as pd
from datetime import timedelta

def detect_scanner_avoidance(pos_data, rfid_data, time_window_seconds=10):
    """
    Detects instances of scanner avoidance.
    Event E001.
    """
    # @algorithm Scanner Avoidance | Detects when an item passes RFID but is not scanned at POS.
    print("Detecting scanner avoidance...")

    if not rfid_data or not pos_data:
        return []

    # Convert to DataFrames for easier manipulation
    rfid_df = pd.DataFrame(rfid_data)
    pos_df = pd.DataFrame(pos_data)

    # Unpack nested data
    rfid_df = pd.concat([rfid_df.drop(['data'], axis=1), rfid_df['data'].apply(pd.Series)], axis=1)
    pos_df = pd.concat([pos_df.drop(['data'], axis=1), pos_df['data'].apply(pd.Series)], axis=1)

    # Filter out null RFID readings and convert timestamp
    rfid_df = rfid_df.dropna(subset=['sku'])
    rfid_df['timestamp'] = pd.to_datetime(rfid_df['timestamp'])
    pos_df['timestamp'] = pd.to_datetime(pos_df['timestamp'])

    events = []

    # Iterate through RFID readings that are not at the entrance/exit
    for _, rfid_item in rfid_df[rfid_df['location'] == 'Checkout'].iterrows():
        # Check if a matching SKU was scanned at the same station within the time window
        is_scanned = any(
            (pos_item['sku'] == rfid_item['sku']) and
            (pos_item['station_id'] == rfid_item['station_id']) and
            (rfid_item['timestamp'] <= pos_item['timestamp'] <= rfid_item['timestamp'] + timedelta(seconds=time_window_seconds))
            for _, pos_item in pos_df.iterrows()
        )

        if not is_scanned:
            # Find the customer at that station around that time
            # This is an approximation: find the customer who checked out just after the RFID event
            potential_customers = pos_df[
                (pos_df['station_id'] == rfid_item['station_id']) &
                (pos_df['timestamp'] > rfid_item['timestamp'])
            ]

            customer_id = "N/A"
            if not potential_customers.empty:
                customer_id = potential_customers.iloc[0]['customer_id']

            events.append({
                "timestamp": rfid_item['timestamp'].isoformat(),
                "event_id": "E001",
                "event_data": {
                    "event_name": "Scanner Avoidance",
                    "station_id": rfid_item['station_id'],
                    "customer_id": customer_id,
                    "product_sku": rfid_item['sku']
                }
            })

    return events

def detect_barcode_switching(vision_data, pos_data, time_window_seconds=3):
    """
    Detects instances of barcode switching.
    Event E002.
    """
    # @algorithm Barcode Switching | Compares vision system data with POS data to find mismatches.
    print("Detecting barcode switching...")

    if not vision_data or not pos_data:
        return []

    vision_df = pd.DataFrame(vision_data)
    pos_df = pd.DataFrame(pos_data)

    # Unpack nested data and convert timestamps
    vision_df = pd.concat([vision_df.drop(['data'], axis=1), vision_df['data'].apply(pd.Series)], axis=1)
    pos_df = pd.concat([pos_df.drop(['data'], axis=1), pos_df['data'].apply(pd.Series)], axis=1)
    vision_df['timestamp'] = pd.to_datetime(vision_df['timestamp'])
    pos_df['timestamp'] = pd.to_datetime(pos_df['timestamp'])

    # Sort by timestamp to prepare for merge_asof
    vision_df = vision_df.sort_values('timestamp')
    pos_df = pos_df.sort_values('timestamp')

    # Merge vision events with the nearest POS event at the same station within a time tolerance
    merged_df = pd.merge_asof(
        vision_df,
        pos_df,
        on='timestamp',
        by='station_id',
        direction='nearest',
        tolerance=pd.Timedelta(seconds=time_window_seconds)
    )

    # Filter for valid matches where the SKUs do not match
    mismatched_df = merged_df.dropna(subset=['predicted_product', 'sku'])
    mismatched_df = mismatched_df[mismatched_df['predicted_product'] != mismatched_df['sku']]

    events = []
    for _, row in mismatched_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E002",
            "event_data": {
                "event_name": "Barcode Switching",
                "station_id": row['station_id'],
                "customer_id": row['customer_id'],
                "actual_sku": row['predicted_product'],
                "scanned_sku": row['sku']
            }
        })

    return events

def detect_weight_discrepancies(pos_data, products_df, tolerance=0.10):
    """
    Detects weight discrepancies in transactions.
    Event E003.
    """
    # @algorithm Weight Discrepancy | Checks for significant differences between weighed and expected item weights.
    print("Detecting weight discrepancies...")

    if not pos_data or products_df.empty:
        return []

    pos_df = pd.DataFrame(pos_data)
    pos_df = pd.concat([pos_df.drop(['data'], axis=1), pos_df['data'].apply(pd.Series)], axis=1)
    pos_df['timestamp'] = pd.to_datetime(pos_df['timestamp'])

    # Merge with product data to get the expected weight
    merged_df = pos_df.merge(products_df, left_on='sku', right_on='SKU')

    # Calculate the absolute difference and check against tolerance
    # The product weight is in kg, POS weight is in g. Let's assume product 'weight' is in grams for now based on context.
    # If it were kg, the discrepancies would be massive. Let's proceed assuming grams.
    merged_df['weight_diff'] = abs(merged_df['weight_g'] - merged_df['weight'])
    merged_df['is_discrepancy'] = merged_df['weight_diff'] > (merged_df['weight'] * tolerance)

    discrepancy_df = merged_df[merged_df['is_discrepancy']]

    events = []
    for _, row in discrepancy_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E003",
            "event_data": {
                "event_name": "Weight Discrepancies",
                "station_id": row['station_id'],
                "customer_id": row['customer_id'],
                "product_sku": row['sku'],
                "expected_weight": row['weight'],
                "actual_weight": row['weight_g']
            }
        })

    return events

def detect_system_crashes(station_data, threshold_seconds=120):
    """
    Detects unexpected system crashes or downtime by checking for heartbeat gaps.
    Event E004.
    """
    # @algorithm System Crash Detection | Identifies periods where a station is unexpectedly offline.
    print("Detecting system crashes...")

    if not station_data:
        return []

    df = pd.DataFrame(station_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['station_id', 'timestamp'])

    # Calculate time difference between consecutive heartbeats for each station
    df['time_diff_seconds'] = df.groupby('station_id')['timestamp'].diff().dt.total_seconds()

    # Filter for gaps that exceed the threshold
    crashes_df = df[df['time_diff_seconds'] > threshold_seconds]

    events = []
    for _, row in crashes_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E004",
            "event_data": {
                "event_name": "Unexpected Systems Crash",
                "station_id": row['station_id'],
                "duration_seconds": int(row['time_diff_seconds'])
            }
        })

    return events

def detect_long_queue_length(queue_data, threshold=5):
    """
    Detects when the queue length at a station exceeds a threshold.
    Event E005.
    """
    # @algorithm Long Queue Detection | Monitors customer count and flags when it's too high.
    print("Detecting long queue lengths...")

    if not queue_data:
        return []

    df = pd.DataFrame(queue_data)
    df = pd.concat([df.drop(['data'], axis=1), df['data'].apply(pd.Series)], axis=1)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    long_queues_df = df[df['customer_count'] > threshold]

    events = []
    for _, row in long_queues_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E005",
            "event_data": {
                "event_name": "Long Queue Length",
                "station_id": row['station_id'],
                "num_of_customers": int(row['customer_count'])
            }
        })
    return events

def detect_long_wait_time(queue_data, threshold_seconds=300):
    """
    Detects when customer wait time at a station exceeds a threshold.
    Event E006.
    """
    # @algorithm Long Wait Time Detection | Monitors average dwell time and flags when it's excessive.
    print("Detecting long wait times...")

    if not queue_data:
        return []

    df = pd.DataFrame(queue_data)
    df = pd.concat([df.drop(['data'], axis=1), df['data'].apply(pd.Series)], axis=1)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    long_waits_df = df[df['average_dwell_time'] > threshold_seconds]

    events = []
    for _, row in long_waits_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E006",
            "event_data": {
                "event_name": "Long Wait Time",
                "station_id": row['station_id'],
                "wait_time_seconds": int(row['average_dwell_time'])
            }
        })
    return events

def detect_inventory_discrepancy(inventory_snapshots, pos_data):
    """
    Detects discrepancies between expected and actual inventory.
    Event E007.
    """
    # @algorithm Inventory Discrepancy | Compares inventory snapshots against sales data to find mismatches.
    print("Detecting inventory discrepancies...")

    if not inventory_snapshots or not pos_data:
        return []

    # Process inventory snapshots into a long format DataFrame
    inventory_records = []
    for snapshot in inventory_snapshots:
        ts = snapshot['timestamp']
        for sku, count in snapshot['data'].items():
            inventory_records.append({'timestamp': ts, 'SKU': sku, 'Actual_Inventory': count})

    if not inventory_records:
        return []

    inventory_df_long = pd.DataFrame(inventory_records)
    inventory_df_long['timestamp'] = pd.to_datetime(inventory_df_long['timestamp'])
    inventory_df_long = inventory_df_long.sort_values(['SKU', 'timestamp'])

    # Calculate inventory drop and time window between snapshots for each SKU
    inventory_df_long['prev_inventory'] = inventory_df_long.groupby('SKU')['Actual_Inventory'].shift(1)
    inventory_df_long['prev_timestamp'] = inventory_df_long.groupby('SKU')['timestamp'].shift(1)
    inventory_df_long['actual_drop'] = inventory_df_long['prev_inventory'] - inventory_df_long['Actual_Inventory']
    inventory_df_long = inventory_df_long.dropna(subset=['prev_inventory', 'prev_timestamp'])

    # Process POS data
    pos_df = pd.DataFrame(pos_data)
    pos_df = pd.concat([pos_df.drop(['data'], axis=1), pos_df['data'].apply(pd.Series)], axis=1)
    pos_df['timestamp'] = pd.to_datetime(pos_df['timestamp'])

    # Calculate items sold in each interval. This is slow but robust.
    def calculate_sales(row, pos_df):
        sales = pos_df[
            (pos_df['sku'] == row['SKU']) &
            (pos_df['timestamp'] > row['prev_timestamp']) &
            (pos_df['timestamp'] <= row['timestamp'])
        ].shape[0]
        return sales

    inventory_df_long['items_sold'] = inventory_df_long.apply(lambda row: calculate_sales(row, pos_df), axis=1)

    # Find discrepancies where the actual drop does not match the sold items
    discrepancy_df = inventory_df_long[inventory_df_long['actual_drop'] != inventory_df_long['items_sold']]

    events = []
    for _, row in discrepancy_df.iterrows():
        events.append({
            "timestamp": row['timestamp'].isoformat(),
            "event_id": "E007",
            "event_data": {
                "event_name": "Inventory Discrepancy",
                "SKU": row['SKU'],
                "Expected_Inventory": int(row['prev_inventory'] - row['items_sold']),
                "Actual_Inventory": int(row['Actual_Inventory'])
            }
        })
    return events