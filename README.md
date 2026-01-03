# MarketFlow

> **End-to-end data engineering pipeline for supermarket analytics using Python, Google Cloud Platform (GCS, BigQuery), and PowerBI.**

MarketFlow simulates a high-velocity retail environment, capturing transaction data, processing it through a data lake, and warehousing it for real-time analytics.

## 🏗 Architecture

- **Ingestion**: Python scripts simulate real-time sales data (Orders, Products, Stores).
- **Data Lake**: **Google Cloud Storage (GCS)** stores raw and processed Parquet files.
- **Data Warehouse**: **Google BigQuery** serves as the analytics engine.
- **Visualization**: **PowerBI** connects to BigQuery for dashboarding.
- **Orchestration**: Python ETL pipelines manage the flow of data.

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Google Cloud Platform Account with billing enabled.
- A GCP Service Account Key (JSON) with permissions for Storage Admin and BigQuery Admin.

### Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/Cole-Khalif/MarketFlow.git
    cd MarketFlow
    ```

2.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: You may need to create a `requirements.txt` with libraries like `google-cloud-storage`, `google-cloud-bigquery`, `pandas`, `faker`)*

3.  **Setup Secrets**
    - Place your `service-account-key.json` in the root directory.
    - **Note:** This file is git-ignored for security.

### 🏃‍♂️ Running the Pipeline

Execute the scripts in the following order:

1.  **Initialize Data Lake**
    ```bash
    python 1_create_datalake.py
    ```
    Creates the storage bucket structure in GCS.

2.  **Generate Data**
    ```bash
    python 2_simulate_data.py
    ```
    Simulates thousands of retail transactions.

3.  **Run ETL Process**
    ```bash
    python 3_etl_pipeline.py
    ```
    Extracts simulated data, transforms it, and uploads to GCS Parquet storage.

4.  **Setup Warehouse**
    ```bash
    python 4_setup_analytics.py
    ```
    Configures BigQuery datasets and external tables.

5.  **Connect Analytics**
    ```bash
    python 5_get_powerbi_key.py
    ```
    Generates the connection string/credentials needed for PowerBI.

## 🛡 Security

- Sensitive keys (`service-account-key.json`) are excluded from source control.
- Data is processed in a secure GCS bucket environment.
