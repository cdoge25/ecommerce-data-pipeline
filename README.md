# Ecommerce Data Pipeline
## 1. Overview
### Introduction
This project showcases a robust data pipeline that automates the process of extraction, transformation, and loading (ETL) of transactional and operational data from an ecommerce platform. Leveraging a suite of modern data stack, the pipeline is designed to centralize data and prepare it for analysis, supporting informed decision-making and business insights.
### Table of contents
- [Ecommerce Data Pipeline](#ecommerce-data-pipeline)
  - [1. Overview](#1-overview)
    - [Introduction](#introduction)
    - [Table of contents](#table-of-contents)
  - [2. Architecture](#2-architecture)
  - [3. Project Structure](#3-project-structure)
## 2. Architecture
<p align="center">
    <img src="assets/diagrams/dataflow.svg" alt="data-flow" style="border-radius: 10px;">
</p>

## 3. Project Structure
```shell
.
├── airflow/              /* Contains DAGs */
├── assets/               /* Contains various project assets: dashboard, images... */
├── dbt_ecom/             /* Contains dbt scripts */
├── docker/               /* Docker configurations and related files of data product */
│   ├── airflow/               
│   ├── postgres/              
│   ├── spark-app/            
│   ├── spark-master/                
│   ├── spark-worker/                
├── .gitignore
├── docker-compose.yaml
├── README.md
└── requirements.txt
```