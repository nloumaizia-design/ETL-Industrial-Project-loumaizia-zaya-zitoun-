import os
import pandas as pd
from datetime import datetime
import re
import sqlite3
from io import StringIO

DATA_DIR = r"C:\Users\n_lou\Desktop\New folder (3)"
SENSOR_PATH = os.path.join(DATA_DIR, "smart_manufacturing_data.csv")
QUALITY_PATH = os.path.join(DATA_DIR, "industrial_fault_detection_data_1000.csv")
DB_PATH = os.path.join(DATA_DIR, "production.db")

# -------------------------------------------------
# EXTRACT: Sensor Data (Fix malformed lines)
# -------------------------------------------------
def extract_sensor_data():
    with open(SENSOR_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    # Insert newline before every timestamp
    fixed = re.sub(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', r'\n\1', content)
    lines = [line.strip() for line in fixed.split('\n') if line.strip()]
    content = '\n'.join(lines)
    
    cols = [
        'timestamp', 'machine_id', 'temperature', 'pressure', 'vibration', 'power',
        'col6', 'col7', 'col8', 'status_label', 'failure_type', 'anomaly_flag', 'maintenance_flag'
    ]
    df = pd.read_csv(StringIO(content), header=None, names=cols, on_bad_lines='skip')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    return df

# -------------------------------------------------
# EXTRACT: Quality Data (Handle embedded headers & map labels)
# -------------------------------------------------
def extract_quality_data():
    with open(QUALITY_PATH, 'r') as f:
        lines = f.readlines()
    
    # Remove lines that are headers
    data_lines = [line for line in lines if not line.startswith('Timestamp')]
    
    content = ''.join(data_lines)
    df = pd.read_csv(StringIO(content), header=None)
    df.columns = [
        'timestamp', 'vibration', 'temperature', 'pressure',
        'rms_vibration', 'mean_temp', 'fault_label'
    ]
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    
    # Map fault_label to defect_type and result
    defect_map = {0: 'Normal', 1: 'Overheating', 2: 'Pressure Drop', 3: 'Vibration Issue'}
    df['defect_type'] = df['fault_label'].map(defect_map).fillna('Unknown')
    df['result'] = df['fault_label'].apply(lambda x: 'fail' if x != 0 else 'pass')
    df['machine_id'] = 'Machine_1'  # inferred (only one machine in this file)
    
    return df[['timestamp', 'machine_id', 'result', 'defect_type']].copy()

# -------------------------------------------------
# TRANSFORM: Clean Sensor Data
# -------------------------------------------------
def clean_sensor_data(df):
    df = df.copy()
    df = df.replace([-999, -1, 999, 'NULL', 'null'], pd.NA)
    
    for col in ['temperature', 'pressure', 'vibration', 'power']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Validate ranges
    df.loc[(df['temperature'] < 0) | (df['temperature'] > 150), 'temperature'] = pd.NA
    df.loc[(df['pressure'] < 0) | (df['pressure'] > 10), 'pressure'] = pd.NA
    df.loc[(df['vibration'] < 0) | (df['vibration'] > 100), 'vibration'] = pd.NA
    
    df = df.sort_values(['machine_id', 'timestamp'])
    df[['temperature', 'pressure', 'vibration', 'power']] = df.groupby('machine_id')[['temperature', 'pressure', 'vibration', 'power']].ffill()
    
    def flag_quality(row):
        if pd.isna(row['temperature']) or pd.isna(row['pressure']) or pd.isna(row['vibration']):
            return 'invalid'
        return 'good'
    df['data_quality'] = df.apply(flag_quality, axis=1)
    return df

# -------------------------------------------------
# TRANSFORM: Standardize & Join
# -------------------------------------------------
def standardize_and_join(sensor_df, quality_df):
    # Standardize
    sensor_df.columns = sensor_df.columns.str.lower()
    sensor_df['machine_id'] = sensor_df['machine_id'].astype(str).str.strip()
    
    # Join on minute-level timestamp + machine_id
    sensor_df['minute'] = sensor_df['timestamp'].dt.floor('min')
    quality_df['minute'] = quality_df['timestamp'].dt.floor('min')
    
    merged = pd.merge(
        sensor_df,
        quality_df[['minute', 'machine_id', 'result', 'defect_type']],
        on=['minute', 'machine_id'],
        how='left'
    )
    merged['quality_status'] = merged['result'].fillna('not_checked')
    return merged.drop(columns=['minute'])

# -------------------------------------------------
# TRANSFORM: Hourly Summary
# -------------------------------------------------
def create_hourly_summary(df):
    df = df.copy()
    df['hour'] = df['timestamp'].dt.floor('h')
    
    summary = df.groupby(['hour', 'machine_id']).agg(
        avg_temperature=('temperature', 'mean'),
        min_temperature=('temperature', 'min'),
        max_temperature=('temperature', 'max'),
        avg_pressure=('pressure', 'mean'),
        avg_vibration=('vibration', 'mean'),
        total_checks=('quality_status', lambda x: x.isin(['pass', 'fail']).sum()),
        defect_count=('quality_status', lambda x: (x == 'fail').sum())
    ).reset_index()
    
    summary['defect_rate'] = (summary['defect_count'] / summary['total_checks'].replace(0, 1)) * 100
    return summary

# -------------------------------------------------
# LOAD: Save to SQLite
# -------------------------------------------------
def save_to_db(sensor_df, quality_df, summary_df):
    conn = sqlite3.connect(DB_PATH)
    
    # Add line_id
    sensor_df['line_id'] = 'Line_1'
    quality_df['line_id'] = 'Line_1'
    summary_df['line_id'] = 'Line_1'
    
    # sensor_readings
    sensor_df[['timestamp', 'line_id', 'machine_id', 'temperature', 'pressure',
               'vibration', 'power', 'data_quality']].to_sql(
        'sensor_readings', conn, if_exists='replace', index=False)
    
    # quality_checks
    quality_df[['timestamp', 'line_id', 'machine_id', 'result', 'defect_type']].to_sql(
        'quality_checks', conn, if_exists='replace', index=False)
    
    # hourly_summary
    summary_df[['hour', 'line_id', 'machine_id', 'avg_temperature', 'min_temperature',
                'max_temperature', 'avg_pressure', 'avg_vibration', 'total_checks',
                'defect_count', 'defect_rate']].to_sql(
        'hourly_summary', conn, if_exists='replace', index=False)
    
    conn.close()

# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    print("🚀 Starting ETL Pipeline...")
    
    # EXTRACT
    sensor_raw = extract_sensor_data()
    quality_raw = extract_quality_data()
    print(f"✅ Loaded {len(sensor_raw)} sensor records and {len(quality_raw)} quality records.")
    
    if sensor_raw.empty:
        print("❌ Sensor data is empty. Aborting.")
        return
    
    # TRANSFORM
    sensor_clean = clean_sensor_data(sensor_raw)
    joined = standardize_and_join(sensor_clean, quality_raw)
    hourly = create_hourly_summary(joined)
    
    # LOAD
    save_to_db(sensor_clean, quality_raw, hourly)
    
    print("✅ ETL pipeline completed successfully!")
    print(f"📁 Database saved to: {DB_PATH}")

if __name__ == "__main__":
    main()
