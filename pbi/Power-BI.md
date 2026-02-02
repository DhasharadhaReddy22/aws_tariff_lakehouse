# Power BI Tariff Analytics Report (PBIP)

## Overview

This Power BI project provides the **analytics and visualization layer** for the **AWS Tariff Lakehouse**. It consumes curated **Gold-layer Iceberg tables** and presents tariff-related trends, comparisons, and metrics in an interactive dashboard.

The project is maintained in **PBIP (Power BI Project) format**, enabling version control, collaboration, and CI/CD–friendly development.

---

## Project Structure

```
pbi/
├── tariff_sample_report.pbip
├── tariff_sample_report.Report/
└── tariff_sample_report.SemanticModel/
```

**Components**

* **`.pbip`**
  Entry point for the Power BI project (opens in Power BI Desktop).

* **`.Report/`**
  Contains report definitions:

  * Pages
  * Visual layouts
  * Interactions and filters

* **`.SemanticModel/`**
  Defines the dataset layer:

  * Tables and relationships
  * Measures (DAX)
  * Columns, hierarchies, and formatting

---

## Data Model

* Built on **Gold-layer lakehouse tables**
* Measures focus on:

  * Tariff values and trends
  * Time-based comparisons
  * Aggregated economic indicators
* Designed for **read performance and clarity**, not raw ingestion

---

## Design Principles

* **Semantic-first modeling**: logic pushed into measures, not visuals
* **Minimal report logic**: transformations handled upstream in the lakehouse
* **PBIP-native**: text-based artifacts for Git diffs and reviews
* **Refresh-safe**: no local-only dependencies (Direct Query)

---

## How to Use

1. Open `tariff_sample_report.pbip` in **Power BI Desktop**
2. Configure the data source as Athena ODBC and using the relevant credentials/profile
3. Refresh the dataset
4. Explore or extend visuals as needed

---

## Role in the Overall System

This Power BI project acts as the **final consumption layer** in the pipeline:

```
S3 + Iceberg (Gold) → Query Engine → Power BI Semantic Model → Dashboard
```

All heavy transformations and business logic are intentionally handled **outside Power BI**, keeping the report lightweight and maintainable.