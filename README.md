#  Bank Customer Churn Prediction - Production ML System

<div align="center">


**A complete end-to-end ML production system for predicting bank customer churn with automated data ingestion, quality validation, real-time predictions, and comprehensive monitoring.**

[Features](#-features) • [Architecture](#-architecture) • [Installation](#-installation) • [Usage](#-usage) • [Dashboards](#-monitoring-dashboards) • [Documentation](#-documentation)

</div>

---

##  Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Project Structure](#-project-structure)
- [Usage Guide](#-usage-guide)
- [Data Pipeline](#-data-pipeline)
- [Monitoring Dashboards](#-monitoring-dashboards)
- [API Documentation](#-api-documentation)
- [Troubleshooting](#-troubleshooting)

---

##  Overview

This project implements a **production-ready machine learning system** for predicting bank customer churn. The system demonstrates industry best practices for ML operations (MLOps), including:

-  Automated data ingestion with quality validation (Great Expectations)
-  Real-time predictions via REST API and scheduled batch processing
-  Comprehensive data quality monitoring with Grafana dashboards
-  Data drift detection for production model monitoring
-  Automated alerts via Microsoft Teams
-  Interactive web interface for on-demand predictions

**Business Use Case:** Predict whether a bank customer will churn (leave the bank) based on their profile, enabling proactive retention strategies.

---

##  Features

###  **Data Quality Management**
- **10 Great Expectations validations** covering:
  - Missing columns and values
  - Data type validation (non-numeric age detection)
  - Range checks (age 0-120, salary 0-1M)
  - Categorical validation (Gender: Male/Female, Geography: France/Spain/Germany)
  - Duplicate detection (rows and customer IDs)
  - Format error detection (ERR_ prefixes)
- **Severity-based classification:** High/Medium/Low criticality
- **HTML validation reports** with detailed error breakdowns
- **Automated Teams notifications** with clickable report links

###  **ML Predictions**
- **Scheduled batch predictions** every 2 minutes on validated data
- **Web interface** for on-demand single and bulk predictions
- **Dual prediction sources:** Webapp (on-demand) + Scheduled (automated)
- **Smart file tracking:** Prevents reprocessing of already-predicted files
- **Error recovery:** Comprehensive logging to `prediction_errors` table

###  **Monitoring & Observability**
- **2 Grafana dashboards** with 16 total panels:
  - **Dashboard 1:** Ingested Data Monitoring (8 panels)
  - **Dashboard 2:** Data Drift & Prediction Issues (8 panels)
- **Color-coded thresholds:** 🟢 Green (healthy), 🟠 Orange (warning), 🔴 Red (critical)
- **Grafana alerts** for critical issues (model stuck, pipeline failures, data quality drops)
- **Real-time updates** with 30-second auto-refresh

###  **Data Drift Detection**
- Age distribution comparison (training vs serving)
- Geography distribution monitoring
- Gender ratio tracking
- Credit score trend analysis
- Average balance monitoring

---

##  Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow 2.7.3 | DAG scheduling (ingestion + prediction) |
| **API** | FastAPI | Model serving & predictions |
| **Database** | PostgreSQL 15 | Store predictions & statistics |
| **Web UI** | Streamlit | User interface for predictions |
| **Monitoring** | Grafana (latest) | Real-time dashboards |
| **Validation** | Great Expectations 0.18.12 | Data quality checks |
| **ML Framework** | Scikit-learn (RandomForest) | Model training & inference |
| **Containerization** | Docker & Docker Compose | Deployment orchestration |
| **Alerts** | Microsoft Teams Webhooks | Critical notifications |

---

##  Prerequisites

Before you begin, ensure you have:

- **Docker Desktop** installed ([Download](https://www.docker.com/products/docker-desktop))
- **Docker Compose** installed (included with Docker Desktop)
- **8GB+ RAM** recommended for all services
- **Ports available:** 3000, 5432, 8000, 8080, 8501
- **Microsoft Teams webhook URL** (optional, for alerts)

**Operating System:** macOS, Linux, or Windows with WSL2

---

##  Installation

### **Step 1: Clone the Repository**

```bash
git clone https://github.com/yourusername/bank-churn-prediction.git
cd bank-churn-prediction
```

### **Step 2: Configure Environment Variables**

Create a `.env` file in the project root:

```bash
# Microsoft Teams Webhook (optional - for alerts)
TEAMS_WEBHOOK=https://your-teams-webhook-url-here

# Database credentials (default values work with docker-compose)
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DB=defence_db
```

**To get a Teams webhook:**
1. Go to your Teams channel → ⋯ → Connectors
2. Search for "Incoming Webhook" → Configure
3. Name it "Data Quality Alerts" → Create
4. Copy the webhook URL to `.env`

### **Step 3: Prepare Training Data**

Place your cleaned dataset here:
```
Data/raw/Churn_Modelling_Cleaned.csv
```

### **Step 4: Generate Test Data with Errors**

The dataset needs intentional errors for validation testing. Run:

```bash
# Generate dataset with 10 error types
python generate_errors.py

# This creates: Data/Errors/dataset_with_errors.csv
```

### **Step 5: Split Dataset into Files**

Split the error dataset into multiple files (simulates continuous data flow):

```bash
cd Data
python split_dataset.py

# This creates 100+ files in Data/raw-data/ folder
# Each file has 10 rows
```

Expected output:
```
[OK] Saved raw-data/raw_split_1.csv (10 rows)
[OK] Saved raw-data/raw_split_2.csv (10 rows)
...
[OK] Saved raw-data/raw_split_100.csv (10 rows)

 Done! All split files generated.
```

### **Step 6: Start All Services**

```bash
# Build and start all containers
docker-compose up --build -d

# Wait for services to initialize (~90 seconds)
sleep 90

# Check all containers are running
docker-compose ps
```

Expected output:
```
NAME                  STATUS
defence_airflow       Up
defence_api           Up
defence_db            Up (healthy)
defence_grafana       Up
defence_streamlit     Up
```

### **Step 7: Verify Services**

```bash
# Check Airflow is ready
curl http://localhost:8080/health

# Check FastAPI is ready
curl http://localhost:8000/health

# Check Grafana is ready
curl http://localhost:3000/api/health
```

All should return `200 OK`.

---

##  Project Structure

```
bank-churn-prediction/
│
├── airflow/                          # Airflow DAGs & config
│   ├── dags/
│   │   ├── data_ingestion_dag.py     # Data validation (every 1 min)
│   │   ├── prediction_dag.py         # Scheduled predictions (every 2 min)
│   │   └── send_alerts.py            # Teams notification helper
│   ├── Dockerfile
│   └── requirements.txt
│
├── fastapi_app/                      # FastAPI service
│   ├── main_api.py                   # API endpoints
│   ├── churn_model.py                # Model inference logic
│   ├── Dockerfile
│   └── requirements.txt
│
├── webapp/                           # Streamlit UI
│   ├── main.py                       # Web interface
│   ├── Dockerfile
│   └── requirements.txt
│
├── database/                         # Database models
│   └── db.py                         # SQLAlchemy schemas
│
├── model/                            # Trained ML model
│   ├── churn_model.pkl               # RandomForest model
│   ├── scaler.pkl                    # StandardScaler
│   ├── Geography_encoder.pkl         # Label encoder
│   ├── Gender_encoder.pkl            # Label encoder
│   └── model_training.py             # Training script
│
├── Data/                             # Data folders
│   ├── raw/                          # Original clean dataset
│   ├── raw-data/                     # Split files (input)
│   ├── good_data/                    # Valid data (output)
│   ├── bad_data/                     # Invalid data (output)
│   ├── archive_raw/                  # Processed files (archive)
│   ├── reports/                      # HTML validation reports
│   └── split_dataset.py              # File splitter script
│
├── grafana/                          # Grafana dashboards
│   ├── dashboards/
│   │   ├── ingestion_monitoring.json       # Dashboard 1
│   │   └── prediction_monitoring.json      # Dashboard 2
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yml         # Auto-provisioning config
│       └── datasources/
│           └── postgres.yml          # PostgreSQL datasource
│
├── docker-compose.yml                # Multi-container orchestration
├── .env                              # Environment variables
└── README.md                         # This file
```

---

##  Usage Guide

### **Access Applications**

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Airflow** | http://localhost:8080 | admin / admin | Monitor DAGs |
| **Streamlit** | http://localhost:8501 | No login | Make predictions |
| **FastAPI** | http://localhost:8000/docs | No login | API documentation |
| **Grafana** | http://localhost:3000 | admin / admin | View dashboards |
| **PostgreSQL** | localhost:5432 | admin / admin | Database access |

---

### **1️ Make Predictions (Streamlit)**

#### **Single Prediction:**

1. Open http://localhost:8501
2. Select **"Single Prediction"** from sidebar
3. Fill in customer details:
   - Credit Score: 650
   - Geography: France
   - Gender: Female
   - Age: 42
   - Tenure: 3
   - Balance: 75000
   - Number of Products: 2
   - Has Credit Card: Yes
   - Is Active Member: Yes
   - Estimated Salary: 80000
4. Click **"Predict"**
5. See result:  "Will not churn" or  "Will churn"

#### **Bulk Prediction:**

1. Select **"Batch Prediction"** from sidebar
2. Prepare CSV file with columns:
   ```
   CreditScore,Geography,Gender,Age,Tenure,Balance,NumOfProducts,HasCrCard,IsActiveMember,EstimatedSalary
   619,France,Female,42,2,0.00,1,1,1,101348.88
   608,Spain,Female,41,1,83807.86,1,0,1,112542.58
   ```
3. Click **"Choose a CSV file"** → Upload
4. Click **"Predict All"**
5. View predictions in dataframe

#### **View Past Predictions:**

1. Select **"Past Predictions"** from sidebar
2. Filter by:
   - **Source:** All / Webapp / Scheduled
   - **Date range:** (optional)
3. Click **"Fetch Past Predictions"**
4. Browse historical predictions

---

### **2️ Monitor Data Ingestion (Airflow)**

1. Open http://localhost:8080
2. Login: `admin` / `admin`
3. You should see 2 DAGs:
   - `data_ingestion_dag` (runs every 1 minute)
   - `prediction_dag` (runs every 2 minutes)

#### **Trigger Data Ingestion Manually:**

1. Click on `data_ingestion_dag`
2. Click **▶ Trigger DAG** (play button)
3. Watch task execution:
   -  **read_data** → Selects random file from `raw-data/`
   -  **validate_data** → Runs 10 Great Expectations checks
   -  **send_alerts** → Generates HTML report + Teams alert
   -  **save_statistics** → Saves to database
   -  **split_and_save** → Moves to `good_data/` or `bad_data/`
4. Check logs for details

#### **View Validation Report:**

After ingestion, you'll receive a Teams alert (if configured) with a link like:
```
http://localhost:8000/reports/a1b2c3d4.html
```

Click to view detailed error breakdown.

#### **Trigger Prediction DAG:**

1. Click on `prediction_dag`
2. Click **▶ Trigger DAG**
3. Watch execution:
   -  **check_for_new_data** → Scans `good_data/` for new files
   -  **make_predictions** → Calls FastAPI to predict
4. If no new files → DAG marked as "skipped" (this is normal!)

---

### **3️ View Monitoring Dashboards (Grafana)**

#### **Access Grafana:**

1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Navigate: **Dashboards** → **Browse**

#### **Dashboard 1: Ingested Data Monitoring**

**Purpose:** Track data quality issues for operations team

**7 Panels:**

1. ** Data Quality Trend** - Valid % vs Invalid % (last 10 min)
   - **Insight:** Is quality improving or degrading?
   -  Green line (Valid %),  Red line (Invalid %)

2. ** Overall Quality Score** - Gauge (0-100%)
   - **Insight:** Current system health
   -  80-100% = Healthy,  50-80% = Warning,  <50% = Critical

3. ** Files Processed** - Count with sparkline (last 1 hour)
   - **Insight:** Is pipeline running?
   -  >5 files = Active,  0 files = Stopped

4. ** Error Criticality** - Stacked bars (last 30 min)
   - **Insight:** Which error types are increasing?
   - Red = High, Orange = Medium, Blue = Low

5. ** Files by Criticality** - Pie chart (last 1 hour)
   - **Insight:** Proportion of problematic files
   - Big red slice = Big problem

6. **� Invalid Rows Trend** - Bar chart (last 1 hour)
   - **Insight:** When did data quality spike?
   - Color changes based on count

7. ** Recent Ingestion Summary** - Table
   - **Insight:** Details of last 10 files
   - Color-coded by quality % and severity

#### **Dashboard 2: Data Drift & Prediction Issues**

**Purpose:** Detect model problems and data drift for ML engineers

**10 Panels:**

1. ** Prediction Volume by Source** - Line graph
   - **Insight:** Pipeline health
   - Blue = Webapp, Green = Scheduled

2. ** Churn Rate** - Gauge
   - **Insight:** Model stuck detection
   -  15-25% = Normal,  0% or 100% = Broken

3. ** Total Predictions** - Stat panel
   - **Insight:** Throughput monitoring

4. ** Age Distribution Drift** - Side-by-side bars
   - **Insight:** Is age distribution shifting?
   - Blue = Baseline, Orange = Current

5. ** Geography Distribution** - Pie chart
   - **Insight:** Categorical drift detection
   - 100% one country = Problem

6. ** Average Age** - Single stat
   - **Insight:** Quick drift check
   - Expected ~40 years

7. ** Average Balance** - Single stat
   - **Insight:** Financial profile drift

8. ** Gender Distribution** - Two lines
   - **Insight:** Gender balance trending

9. ** Recent Prediction Errors** - Table
   - **Insight:** Debug pipeline failures

10. ** Prediction Class Distribution** - Stacked histogram
    - **Insight:** Model predicting both classes?

11. ** Credit Score Trend** - Multi-line graph
    - **Insight:** Customer quality monitoring

---

### **4️ Query Database Directly**

```bash
# Connect to PostgreSQL
docker exec -it defence_db psql -U admin -d defence_db

# View ingestion statistics
SELECT * FROM ingestion_statistics ORDER BY created_at DESC LIMIT 10;

# View predictions
SELECT * FROM predictions ORDER BY created_at DESC LIMIT 10;

# View prediction errors
SELECT * FROM prediction_errors ORDER BY timestamp DESC LIMIT 10;

# Exit
\q
```

---

##  Data Pipeline

### **Ingestion Pipeline (Every 1 Minute)**

```
1. Read random CSV from raw-data/
   ↓
2. Run 10 Great Expectations validations:
   ✓ Column existence (high severity)
   ✓ Missing values (medium severity)
   ✓ Non-numeric age (high severity)
   ✓ Age range 0-120 (high severity)
   ✓ Salary range 0-1M (high severity)
   ✓ Gender: Male/Female (high severity)
   ✓ Geography: France/Spain/Germany (medium severity)
   ✓ Duplicate rows (medium severity)
   ✓ Duplicate customer IDs (medium severity)
   ✓ ERR_ prefix format errors (medium severity)
   ↓
3. Generate HTML validation report
   ↓
4. Send Teams alert (if errors > 0)
   ↓
5. Split data:
   • All valid → good_data/
   • All invalid → bad_data/
   • Mixed → split into 2 files
   ↓
6. Archive original → archive_raw/
   ↓
7. Save statistics to database:
   • file_name
   • total_rows
   • valid_rows
   • invalid_rows
   • criticality (high/medium/low)
   • report_path
```

### **Prediction Pipeline (Every 2 Minutes)**

```
1. Check FastAPI health
   ↓
2. Scan good_data/ for new files
   ↓
3. Query database: Which files already processed?
   ↓
4. For each NEW file:
   ├─ Read CSV in chunks (500 rows)
   ├─ Clean data (remove inf, nan)
   ├─ Call POST /predict
   ├─ Log success/failure to prediction_errors
   ↓
5. Files stay in good_data/ (not deleted)
   Database tracks processing status
```

**Why files aren't deleted:**
- Allows reprocessing if needed
- Audit trail
- Database tracks `source_file` to prevent duplicates

---

##  Monitoring Dashboards

### **Dashboard 1: Ingested Data Monitoring**

| Panel | Type | Time Range | Insight | Colors |
|-------|------|------------|---------|--------|
| Data Quality Trend | Line graph | 10 min | Quality improving/degrading? | 🟢 Green (valid), 🔴 Red (invalid) |
| Quality Score | Gauge | 10 min | Current health status | 🟢 >80%, 🟠 50-80%, 🔴 <50% |
| Files Processed | Stat | 1 hour | Pipeline active? | 🟢 >5, 🟡 1-5, 🔴 0 |
| Error Criticality | Stacked bars | 30 min | Error type distribution | 🔴 High, 🟠 Medium, 🔵 Low |
| Files by Criticality | Pie chart | 1 hour | Severity proportions | 🔴🟠🟢 |
| Invalid Rows Trend | Bar chart | 1 hour | Spike detection | Color by count |
| Ingestion Summary | Table | 1 hour | File-level details | Color-coded cells |

### **Dashboard 2: Data Drift & Prediction Issues**

| Panel | Type | Time Range | Insight | Colors |
|-------|------|------------|---------|--------|
| Prediction Volume | Line graph | 2 hours | Pipeline health | Blue (webapp), Green (scheduled) |
| Churn Rate | Gauge | 1 hour | Model stuck? | 🟢 15-25%, 🔴 0% or 100% |
| Total Predictions | Stat | 1 hour | Throughput | 🟢 >100, 🟡 50-100, 🔴 <10 |
| Age Distribution | Bar chart | 2h vs 10min | Age drift detection | Blue (baseline), Orange (current) |
| Geography Dist. | Pie chart | 1 hour | Categorical drift | Blue/Red/Yellow |
| Average Age | Stat | 1 hour | Quick drift check | 🟢 35-50, 🟠 30-35/50-55, 🔴 <30/>55 |
| Average Balance | Stat | 1 hour | Financial drift | 🟢 60K-100K, 🟠 outside |
| Gender Dist. | Line graph | 2 hours | Gender balance | Blue (male), Purple (female) |
| Prediction Errors | Table | 30 min | Error debugging | Color by type |
| Class Distribution | Stacked bars | 2 hours | Model predicting both? | 🔴 Churn, 🟢 No churn |
| Credit Score | Line graph | 2 hours | Quality degradation? | Blue/Red/Green lines |

### **Grafana Alerts Configured**

| Alert | Condition | Trigger | Message |
|-------|-----------|---------|---------|
| High Invalid Data | Invalid % > 50% | 5 min |  Critical data quality drop |
| Critical Quality | Quality < 50% | 2 min |  Immediate action required |
| Pipeline Stopped | 0 files | 10 min |  No files ingested |
| High Severity Errors | >5 high errors | 10 min |  Multiple critical errors |
| All Files Errors | 100% bad | 10 min |  NO valid data |
| Prediction Stopped | 0 predictions | 10 min |  Pipeline broken |
| Model Stuck | Churn 0-1% or 99-100% | 5 min |  Model predicting one class |
| Age Drift | Age <30 or >55 | 10 min |  Data distribution changed |

---

##  API Documentation

### **FastAPI Endpoints**

Access interactive docs: http://localhost:8000/docs

#### **POST /predict**

Make batch predictions.

**Request:**
```bash
curl -X POST "http://localhost:8000/predict?source=webapp&source_file=manual.csv" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "CreditScore": 619,
      "Geography": "France",
      "Gender": "Female",
      "Age": 42,
      "Tenure": 2,
      "Balance": 0.00,
      "NumOfProducts": 1,
      "HasCrCard": 1,
      "IsActiveMember": 1,
      "EstimatedSalary": 101348.88
    }
  ]'
```

**Response:**
```json
{
  "prediction": 0,
  "prediction_label": "Will not churn",
  "source": "webapp",
  "source_file": "manual.csv"
}
```

**Multi-prediction response:**
```json
{
  "predictions": [
    {"prediction": 0, "prediction_label": "Will not churn", ...},
    {"prediction": 1, "prediction_label": "Will churn", ...}
  ],
  "count": 2
}
```

#### **GET /health**

Check API status.

**Request:**
```bash
curl http://localhost:8000/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "churn-prediction-api",
  "timestamp": "2025-11-29T10:30:00.000000"
}
```

#### **GET /past-predictions**

Query prediction history.

**Parameters:**
- `start_date` (optional): ISO datetime
- `end_date` (optional): ISO datetime
- `source` (optional): `webapp`, `scheduled`, or `all` (default)
- `limit` (optional): Max records (1-1000, default 200)

**Request:**
```bash
curl "http://localhost:8000/past-predictions?source=webapp&limit=10"
```

**Response:**
```json
{
  "past_predictions": [
    {
      "id": 1,
      "credit_score": 619,
      "geography": "France",
      "gender": "Female",
      "age": 42,
      "prediction": 0,
      "prediction_label": "Will not churn",
      "created_at": "2025-11-29 10:30:00",
      "source": "webapp",
      "source_file": null
    }
  ],
  "count": 1
}
```

#### **GET /reports/{filename}**

View HTML validation reports.

**Request:**
```
http://localhost:8000/reports/a1b2c3d4e5f6.html
```

---

##  Database Schema

### **Table: `predictions`**

Stores all model predictions.

```sql
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    credit_score FLOAT,
    geography VARCHAR(50),
    gender VARCHAR(10),
    age FLOAT,
    tenure FLOAT,
    balance FLOAT,
    num_of_products INTEGER,
    has_cr_card INTEGER,
    is_active_member INTEGER,
    estimated_salary FLOAT,
    prediction INTEGER,              -- 0 or 1
    source VARCHAR(20),              -- 'webapp' or 'scheduled'
    source_file VARCHAR(255),        -- filename (for scheduled)
    created_at TIMESTAMP DEFAULT NOW()
);
```

### **Table: `ingestion_statistics`**

Tracks data quality metrics.

```sql
CREATE TABLE ingestion_statistics (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    total_rows INTEGER,
    valid_rows INTEGER,
    invalid_rows INTEGER,
    criticality VARCHAR(10),         -- 'high', 'medium', 'low'
    report_path TEXT,                -- URL to HTML report
    created_at TIMESTAMP DEFAULT NOW()
);
```

### **Table: `prediction_errors`**

Logs prediction pipeline errors.

```sql
CREATE TABLE prediction_errors (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    file_name VARCHAR(255),
    error_type VARCHAR(50),          -- 'api_error', 'column_error', etc.
    error_message TEXT
);
```

---

##  Troubleshooting

### **Issue: Airflow DAGs not showing**

```bash
# Check Airflow logs
docker logs defence_airflow | tail -50

# Verify DAG files exist
docker exec defence_airflow ls /opt/airflow/dags/

# Check for syntax errors
docker exec defence_airflow python -m py_compile /opt/airflow/dags/data_ingestion_dag.py
```

### **Issue: Grafana shows "No data"**

```bash
# 1. Check database has data
docker exec defence_db psql -U admin -d defence_db -c "SELECT COUNT(*) FROM ingestion_statistics;"

# 2. Verify datasource connection
# Go to: Grafana → Configuration → Data Sources → defence_db → Save & Test
# Should show: Database Connection OK

# 3. Check datasource URL in docker-compose.yml
# Should be: url: defence_db:5432

# 4. Restart Grafana
docker-compose restart grafana
```

### **Issue: Prediction DAG always skips**

```bash
# This is NORMAL if no new files!

# Check for new files
ls Data/good_data/

# Check which files are processed
docker exec defence_db psql -U admin -d defence_db -c \
  "SELECT DISTINCT source_file FROM predictions WHERE source='scheduled' LIMIT 10;"

# Manually trigger ingestion first
# Airflow UI → data_ingestion_dag → Trigger DAG
# Wait 2 minutes → prediction_dag will process new file
```

### **Issue: Teams alerts not sending**

```bash
# 1. Verify webhook URL is set
docker exec defence_airflow env | grep TEAMS_WEBHOOK

# 2. Test webhook manually
curl -X POST $TEAMS_WEBHOOK \
  -H "Content-Type: application/json" \
  -d '{"text":"Test alert from terminal"}'

# 3. Check Airflow logs
docker logs defence_airflow | grep -i teams
```

### **Issue: "Permission denied" errors**

```bash
# Fix file permissions
chmod -R 755 Data/
chmod -R 755 grafana/

# Restart containers
docker-compose restart
```

### **Issue: FastAPI /predict fails**

```bash
# Check FastAPI logs
docker logs defence_api | tail -50

# Test model loading
docker exec defence_api python -c "from churn_model import preprocess_and_predict; print('OK')"

# Verify model files exist
docker exec defence_api ls /workspace/model/
```

---

##  Configuration

### **Change DAG Schedules**

Edit `airflow/dags/data_ingestion_dag.py`:
```python
# Change from every 1 minute to every 5 minutes
schedule="*/5 * * * *"
```

Edit `airflow/dags/prediction_dag.py`:
```python
# Change from every 2 minutes to every 10 minutes
schedule_interval=timedelta(minutes=10)
```

Restart Airflow:
```bash
docker-compose restart airflow
```

### **Adjust Great Expectations Thresholds**

Edit `airflow/dags/data_ingestion_dag.py`:

```python
# Change age range
check("out_of_range_age", "high", 
      lambda: ge_df.expect_column_values_to_be_between("Age", 18, 100))

# Change salary range
check("out_of_range_income", "high",
      lambda: ge_df.expect_column_values_to_be_between("EstimatedSalary", 10000, 500000))
```

### **Change Grafana Alert Thresholds**

Edit dashboard JSON files:

```json
"thresholds": {
  "steps": [
    {"color": "red", "value": null},
    {"color": "orange", "value": 60},    // Changed from 50
    {"color": "green", "value": 85}      // Changed from 80
  ]
}
```

Re-import dashboard or update via Grafana UI.

---

##  Documentation

### **Project Requirements (Original Assignment)**

This project fulfills all requirements for the "Data Science in Production" course:

 **User Interface** - Streamlit webapp with single/batch prediction + past predictions  
 **Model API** - FastAPI with `/predict`, `/health`, `/past-predictions` endpoints  
 **Database** - PostgreSQL with 3 tables (predictions, statistics, errors)  
 **Prediction Job** - Airflow DAG running every 2 minutes  
 **Ingestion Job** - Airflow DAG running every 1 minute with Great Expectations  
 **Monitoring** - 2 Grafana dashboards (17 total panels)  
 **Data Quality** - 10 validation checks with severity classification  
 **Alerts** - Teams notifications + Grafana alerts  
 **Data Splitting** - Good/bad data separation  
 **Error Types** - 10+ intentional errors generated  

### **Additional Features (Beyond Requirements)**

 **Comprehensive error logging** to `prediction_errors` table  
 **HTML validation reports** with clickable Teams links  
 **Data drift detection** (age, geography, gender, credit score)  
 **Model health monitoring** (stuck detection, volume tracking)  
 **Auto-provisioned Grafana** with datasource + dashboards  
 **Color-coded dashboards** (green/orange/red thresholds)  
 **Smart file tracking** (prevents reprocessing)  
 **Health endpoints** for all services  

---

##  Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### **Development Setup**

```bash
# Install Python dependencies locally (optional)
pip install -r airflow/requirements.txt
pip install -r fastapi_app/requirements.txt
pip install -r webapp/requirements.txt

# Run tests (if you add them)
pytest tests/
```

---
<div align="center">


</div>

---

##  Quick Start Commands

```bash
# Clone repo
git clone https://github.com/yourusername/bank-churn-prediction.git
cd bank-churn-prediction

# Setup
echo "TEAMS_WEBHOOK=your-webhook-url" > .env
cd Data && python split_dataset.py && cd ..

# Start
docker-compose up -d

# Verify
docker-compose ps
curl http://localhost:8000/health
curl http://localhost:8080/health

# Access
open http://localhost:8501          # Streamlit
open http://localhost:8080          # Airflow
open http://localhost:3000          # Grafana
```

**That's it! Your ML production system is running! **
