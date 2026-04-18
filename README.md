# fabric-data-engineering-portfolio


# Project 1 — Medallion Architecture Pipeline

## Overview
End-to-end ETL pipeline built on Microsoft Fabric using the 
Bronze → Silver → Gold medallion architecture.

## Dataset
Sample Employee Data (EmployeeID, Name, Department, Salary)

## Architecture
- **Bronze** — Raw CSV ingested as Delta table
- **Silver** — Cleaned data with SalaryBand classification
- **Gold** — Department-level aggregations (avg, max, min salary)

## Tech Stack
- Microsoft Fabric Lakehouse
- PySpark
- Delta Lake
- Fabric Data Pipeline
- Office 365 Email Alerts

## Pipeline Features
- Scheduled daily at 8AM
- Success/Failure email notifications
- End-to-end automated execution

## Results
| Department | Avg Salary | Max Salary |
|------------|------------|------------|
| IT         | 71,000     | 72,000     |
| Finance    | 62,500     | 63,000     |
| Sales      | 60,500     | 61,000     |
| Marketing  | 58,500     | 59,000     |
| HR         | 55,500     | 56,000     |
