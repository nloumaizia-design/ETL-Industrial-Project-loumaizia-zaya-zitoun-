# ETL Pipeline Project – Industrial Fault Detection

This project implements an ETL (Extract, Transform, Load) pipeline for industrial sensor and quality data.

## 📁 Included Files
- `etl_pipeline.py`: Python script that processes raw CSV files and generates a SQLite database.
- `production.db`: Output SQLite database containing three tables:
  - `sensor_readings`
  - `quality_checks`
  - `hourly_summary`

## 📊 Data Sources
The pipeline uses two datasets from Kaggle:
- [Smart Manufacturing Data](https://www.kaggle.com/datasets/ziya07/smart-manufacturing-data)
- [Industrial IoT Fault Detection Dataset](https://www.kaggle.com/datasets/ziya07/industrial-iot-fault-detection-dataset)

## 🚀 How to Run
1. Install required packages:
   ```bash
   pip install pandas