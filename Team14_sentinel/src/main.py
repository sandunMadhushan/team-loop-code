import os
import json
from data_loader import load_csv_data
import event_detector

# Construct robust paths to the data and output directories.
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
DATA_DIR = os.path.join(project_root, '..', 'data/input')
OUTPUT_DIR = os.path.join(project_root, 'evidence/output/test') # Default to test

def load_streaming_data(data_dir):
    """Loads all JSONL streaming data into a dictionary of lists."""
    streams = {}
    for filename in os.listdir(data_dir):
        if filename.endswith('.jsonl'):
            stream_name = filename.replace('.jsonl', '')
            streams[stream_name] = []
            with open(os.path.join(data_dir, filename), 'r') as f:
                for line in f:
                    streams[stream_name].append(json.loads(line))
    print("Successfully loaded all streaming data files.")
    return streams

def run_pipeline():
    """
    Main function to run the entire data processing pipeline.
    """
    print("Starting Project Sentinel Data Processing Pipeline...")

    # Load static and streaming data
    try:
        products_df, customers_df = load_csv_data(DATA_DIR)
        streaming_data = load_streaming_data(DATA_DIR)
    except FileNotFoundError:
        print(f"Error: Could not find data files in {os.path.abspath(DATA_DIR)}")
        return

    all_events = []

    # Run all event detectors
    all_events.extend(event_detector.detect_scanner_avoidance(streaming_data.get('pos_transactions'), streaming_data.get('rfid_readings')))
    all_events.extend(event_detector.detect_barcode_switching(streaming_data.get('product_recognition'), streaming_data.get('pos_transactions')))
    all_events.extend(event_detector.detect_weight_discrepancies(streaming_data.get('pos_transactions'), products_df))
    all_events.extend(event_detector.detect_system_crashes(streaming_data.get('queue_monitoring'))) # Example, might need more streams
    all_events.extend(event_detector.detect_long_queue_length(streaming_data.get('queue_monitoring')))
    all_events.extend(event_detector.detect_long_wait_time(streaming_data.get('queue_monitoring')))
    all_events.extend(event_detector.detect_inventory_discrepancy(streaming_data.get('inventory_snapshots'), streaming_data.get('pos_transactions')))

    # Save events to output file
    output_path = os.path.join(OUTPUT_DIR, 'events.jsonl')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, 'w') as f:
        for event in all_events:
            f.write(json.dumps(event) + '\n')

    print(f"Pipeline complete. Found {len(all_events)} events.")
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    run_pipeline()