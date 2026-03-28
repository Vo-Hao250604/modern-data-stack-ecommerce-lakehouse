# 🚀 Modern Data Stack Ecommerce Lakehouse

## 📌 Introduction

This project presents the implementation of a **Data Lakehouse architecture** using the **Modern Data Stack (MDS)** to support advanced analytics for an e-commerce system.

The system is designed to process large-scale data and provide insights into:

* Customer behavior
* Product performance
* Sales trends

It enables **data-driven decision-making** through scalable pipelines and interactive dashboards.

---

## 🎯 Project Objectives

* Build a **scalable Data Lakehouse architecture**
* Apply **Modern Data Stack tools** in real-world scenarios
* Design and implement **end-to-end data pipelines**
* Perform:

  * Customer analytics
  * Product analytics
  * Revenue analysis
* Support **business intelligence dashboards**
* Lay foundation for **Machine Learning & AI integration**

---

## 🏗️ System Architecture

### 🔹 Architecture Overview

The system follows the **Medallion Architecture**:

```
Data Sources → Bronze → Silver → Gold → BI / ML
```

### 🔹 Layers Description

* **Bronze Layer**: Raw data ingestion from e-commerce sources
* **Silver Layer**: Data cleaning, transformation, normalization
* **Gold Layer**: Aggregated data for analytics and reporting

---

## ⚙️ Tech Stack

### ☁️ Cloud & Storage

* AWS S3 – Data Lake storage
* AWS Glue – Metadata management
* AWS Athena – Query engine

### 🔄 Data Processing

* Apache Spark (PySpark) – Distributed data processing
* Pandas – Data manipulation

### ⏱️ Orchestration

* Apache Airflow (CeleryExecutor)
* Redis + PostgreSQL (Airflow backend)

### 📊 Visualization

* Apache Superset – BI dashboards

### 🤖 Machine Learning

* Scikit-learn – Predictive models

---

## 🐳 Infrastructure (Docker)

The system uses **Docker Compose** to deploy Airflow cluster:

### Services:

* Airflow Webserver (port 8080)
* Airflow Scheduler
* Redis (message broker)
* PostgreSQL (metadata DB)

### Run system:

```bash
docker-compose up -d
```

Access Airflow UI:

```
http://localhost:8080
```

---

## 📂 Project Structure

```
├── dags/                  # Airflow DAGs (ETL pipelines)
├── data/
│   ├── bronze/            # Raw data
│   ├── silver/            # Cleaned data
│   └── gold/              # Aggregated data
│
├── spark/                 # Spark jobs
├── notebooks/             # Data analysis & ML
├── dashboards/            # Superset dashboards
├── configs/               # Config files
│
├── docker-compose.yaml
└── README.md
```

---

## 🔄 Data Pipeline

### 1. Data Ingestion

* Load data from e-commerce dataset (orders, customers, products)

### 2. Bronze Layer

* Store raw data in Data Lake (S3 / local)

### 3. Silver Layer

* Clean data (remove nulls, standardize format)
* Transform schema

### 4. Gold Layer

* Build fact & dimension tables
* Aggregate metrics:

  * Revenue
  * Orders
  * Customer behavior

### 5. Visualization

* Dashboards in Superset:

  * Top selling products
  * Customer segmentation
  * Revenue analysis

---

## 📊 Key Features

### 👤 Customer Analytics

* Customer segmentation (RFM)
* Purchase behavior analysis

### 📦 Product Analytics

* Best-selling products
* Product performance tracking

### 💰 Sales Analytics

* Revenue trends
* Monthly performance

### 🤖 Machine Learning Models

* Delivery time prediction
* Monthly revenue prediction
* Product rating prediction

---

## 🚀 Getting Started

### 1. Clone repository

```bash
git clone https://github.com/your-username/modern-data-stack-ecommerce-lakehouse.git
cd modern-data-stack-ecommerce-lakehouse
```

### 2. Start system (Docker)

```bash
docker-compose up -d
```

### 3. Run Airflow

* Open: http://localhost:8080
* Trigger DAGs manually or schedule

---

## 📈 Example Dashboards

* Top selling products
* Customer purchase distribution
* Revenue statistics

---

## 🧠 Future Improvements

* Real-time streaming (Kafka)
* Data quality monitoring
* CI/CD pipeline for data workflows
* Advanced ML models (recommendation system)
* Integration with AI systems

---

## 👨‍💻 Author

* **Võ Thanh Hào**
* Student ID: 2251050026
* Major: Information Technology
* University: Ho Chi Minh City Open University

---

## 📜 License

This project is developed for **academic purposes**.
