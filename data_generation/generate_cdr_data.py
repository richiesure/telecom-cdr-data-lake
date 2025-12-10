import csv
import random
from datetime import datetime, timedelta

def generate_cdr_data(num_records=10000, output_file='cdr_data.csv'):
    call_types = ['voice', 'sms', 'data']
    call_statuses = ['completed', 'failed', 'busy', 'no_answer']
    cell_towers = [f'TOWER_{i:04d}' for i in range(1, 101)]
    start_date = datetime(2024, 11, 1)
    
    print(f"Generating {num_records} CDR records...")
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'record_id', 'msisdn', 'imsi', 'call_type', 'call_status',
            'call_duration_seconds', 'data_usage_mb', 'cell_tower_id',
            'latitude', 'longitude', 'call_timestamp', 'call_date', 'billing_amount'
        ])
        
        for i in range(1, num_records + 1):
            msisdn = f"+44{random.randint(7000000000, 7999999999)}"
            imsi = f"{random.randint(100000000000000, 999999999999999)}"
            call_type = random.choice(call_types)
            call_status = random.choice(call_statuses)
            duration = random.randint(10, 3600) if call_status == 'completed' else 0
            data_usage = round(random.uniform(0.5, 500.0), 2) if call_type == 'data' else 0.0
            cell_tower = random.choice(cell_towers)
            latitude = round(random.uniform(50.0, 55.0), 6)
            longitude = round(random.uniform(-5.0, 2.0), 6)
            random_days = random.randint(0, 29)
            random_seconds = random.randint(0, 86400)
            call_timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)
            call_date = call_timestamp.strftime('%Y-%m-%d')
            
            if call_type == 'voice':
                billing = round(duration * 0.01, 2)
            elif call_type == 'sms':
                billing = 0.05
            elif call_type == 'data':
                billing = round(data_usage * 0.02, 2)
            else:
                billing = 0.0
            
            writer.writerow([
                f'CDR_{i:08d}', msisdn, imsi, call_type, call_status,
                duration, data_usage, cell_tower, latitude, longitude,
                call_timestamp.strftime('%Y-%m-%d %H:%M:%S'), call_date, billing
            ])
    
    print(f"✓ Generated {num_records} CDR records")
    print(f"✓ Saved to: {output_file}")

if __name__ == "__main__":
    generate_cdr_data(num_records=10000, output_file='cdr_data.csv')
