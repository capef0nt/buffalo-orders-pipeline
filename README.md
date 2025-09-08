# Buffalo Orders Pipeline

## Project Overview
**Solo**, an e-commerce company, faces challenges with delayed orders and occasional inventory shortages. To improve their supply chain and maintain customer satisfaction, Solo wants to **analyze and visualize order and logistics data** from their shipping partner.  

The goal of this project is to build a **production-ready ETL pipeline** that:

- Continuously monitors incoming orders from Buffalo’s API  
- Stores raw order data for auditing and historical analysis  
- Provides the foundation for downstream analytics to:
  - Predict potential delivery delays  
  - Optimize inventory levels  
  - Alert customers proactively when delays occur  

This project is the first step in creating a **data-driven supply chain monitoring system** that allows Solo to make timely decisions and improve operational efficiency.

---

## Technical Solution
To address this problem, I implement a **modular ETL pipeline** with the following components:

- **Ingestion:** Extract all orders from the Buffalo API and store raw JSON directly in **PostgreSQL**  
- **Orchestration:** Use Apache Airflow DAGs to schedule and manage the pipeline  
- **Future ETL stages:** 
  - Transform: Clean and filter order data  
  - Load: Push relevant fields to SQL tables for analytics and reporting  
  - Visualize: Connect to Power BI to monitor delays and inventory  

> **Note:** PostgreSQL is used as the single source of truth for both raw and transformed data, ensuring persistence, consistency, and easy integration with analytics tools. This design also supports future deployment to cloud platforms.

---

## Tech Stack
- **Python 3.11** – API scripting and data handling  
- **PostgreSQL 15** – raw and transformed data storage  
- **Apache Airflow 2.x** – DAG orchestration and scheduling  
- **Docker & Docker Compose** – containerized deployment  
- **Requests, PyCryptodome, python-dotenv** – secure API communication  

---

## Features (Current Stage)
- Securely logs into Buffalo API using RSA encryption  
- Fetches all order IDs and retrieves full order details  
- Stores raw JSON directly in **PostgreSQL** for auditing and downstream processing  
- Airflow DAG with tasks:
  1. `get_order_ids` — fetches all order IDs  
  2. `fetch_and_store_raw` — retrieves order details and upserts into PostgreSQL  
- Retry mechanisms, logging, and scheduling included  
- Fully Dockerized for easy local and production deployment  
- Structured to allow **future cloud deployment** with minimal changes  

---

## Architecture
*(Airflow DAG → PostgreSQL → Analytics/Power BI)*

