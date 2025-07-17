# Final-Project---E-commerce-Product-Demand-Sales-Analysis

Project Statement:

E-commerce Product Demand & Sales Analysis
ETL Problem:

Ingest order and inventory data from APIs and ADLS Gen2 using Azure Data Factory.
Clean, join, and aggregate demand data using Azure Databricks (with Delta Lake).
Use Azure DevOps Repos to manage all transformations as notebooks/SQL scripts.

ML Problem:

Train time-series forecasting models (e.g., Prophet or ARIMA) in Databricks to predict product-wise demand.
Register model with MLflow, promote using Azure DevOps release pipeline, and monitor drift.

Project Overview

In the dynamic landscape of e-commerce, accurate demand forecasting is paramount for optimizing inventory management, supply chain efficiency, and marketing strategies. This project establishes a robust, end-to-end data and MLOps pipeline designed to address these challenges. The primary objective is to ingest raw e-commerce transaction data, transform it into a refined format, apply advanced machine learning models for predicting future demand, and automate the entire model lifecycle from training to deployment. The ultimate goal is to provide actionable insights through accessible visualizations, enabling data-driven decision-making for business stakeholders.

Workflow Overview:

Data Ingestion (Bronze Layer): Raw e-commerce data is securely brought into ADLS Gen2.
Data Transformation (Silver Layer): Raw data undergoes cleansing, enrichment, and aggregation, transforming it into a structured, queryable format.
Forecasting & MLflow Tracking (Gold Layer): Machine learning models consume the Silver layer data to generate demand forecasts. All model training details and artifacts are meticulously tracked in MLflow.
Model Promotion (MLOps CI/CD): A streamlined process, automated by Azure DevOps, moves validated models from development to production stages.
Pipeline Orchestration: Azure Data Factory ensures the sequential and scheduled execution of all data processing and model training steps.
Data Visualization: The final, curated forecasts from the Gold layer are ingested into Azure Data Explorer for interactive analysis and dashboarding, providing business users with actionable insights.
