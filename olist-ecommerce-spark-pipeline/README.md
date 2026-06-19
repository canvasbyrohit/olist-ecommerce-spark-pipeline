# Olist Brazilian E-Commerce — Apache Spark Data Integration Pipeline

**Student Name    :** [Your Full Name]  
**Student ID      :** [Your Student ID]  
**Module          :** Big Data Management / Data Integration  
**Dataset Source  :** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce  
**GitHub Repo     :** [Your GitHub Repository URL]  
**Video Demo      :** [Your Video URL — YouTube / Google Drive]  

---

## Project Overview

This project designs and implements a complete Apache Spark ETL pipeline
for the Olist Brazilian E-Commerce Public Dataset — a real-world
multi-table relational dataset of 100,000+ orders placed on the Olist
marketplace between 2016 and 2018.

Nine source CSV files are ingested, cleaned, schema-aligned, and
integrated into a unified analytical Parquet dataset. Six business
questions are answered using Spark SQL and the DataFrame API, and
eight visualisations are produced to support data-driven
decision-making for a fictional e-commerce analytics consultancy.

---

## Dataset

| Property        | Detail                                                      |
|-----------------|-------------------------------------------------------------|
| Source          | Kaggle — Olist Brazilian E-Commerce Public Dataset          |
| URL             | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Tables          | 9 CSV files                                                 |
| Total records   | ~1.6 million rows across all tables                         |
| Date range      | September 2016 – October 2018                               |
| License         | CC BY-NC-SA 4.0                                             |

---

## Business Problem

Olist's operational data is fragmented across 9 relational CSV files.
Without an integrated analytics layer, business teams cannot answer
cross-functional questions about sales performance, delivery quality,
customer satisfaction, or seller reliability.

This project builds a unified data pipeline that enables:
- Revenue analysis by product category and state
- Delivery performance measurement and satisfaction correlation
- Payment behaviour and instalment pattern analysis
- Seller quality monitoring and underperformer identification

---

## Pipeline Architecture

```
Raw Layer       (9 source CSVs — orders, items, customers, products,
                 sellers, payments, reviews, geolocation, category translation)
      │
      ▼
Processing Layer (PySpark DataFrame API — clean, align, transform)
      │   - Timestamp casting with explicit format strings
      │   - Deduplication per primary key
      │   - Null handling (drop / fill / retain)
      │   - Invalid value filtering
      │   - Schema alignment (Portuguese → English categories)
      │   - Derived columns (delivery_delay_days, is_late, freight_ratio)
      ▼
Curated Layer   (Parquet outputs — snappy compressed)
      │   - olist_integrated.parquet   (~112K rows, 34 columns)
      │   - olist_aggregated.parquet   (pre-grouped summary)
      ▼
Analytics Layer (Spark SQL + Pandas/Matplotlib visualisations)
      │   - 6 primary business questions answered
      │   - Window functions: rolling avg, revenue share %, row deduplication
      │   - 8 professional visualisation charts
```

---

## Repository Structure

```
olist-ecommerce-spark-pipeline/
├── README.md                          ← this file
├── requirements.txt                   ← Python dependencies
├── .gitignore                         ← excludes data files and Parquet
│
├── notebooks/
│   └── olist_spark_pipeline.ipynb    ← full 55-cell pipeline notebook
│
├── src/
│   ├── __init__.py
│   ├── config.py                      ← all paths and constants
│   ├── schemas.py                     ← all 9 explicit schema definitions
│   ├── ingest.py                      ← ingestion functions
│   ├── clean.py                       ← cleaning functions
│   ├── integrate.py                   ← integration functions
│   ├── pipeline.py                    ← ETL orchestrator (single entry point)
│   └── analytics.py                   ← Spark SQL query functions
│
├── data/
│   ├── raw/                           ← place Kaggle CSVs here (gitignored)
│   └── processed/                     ← Parquet outputs written here (gitignored)
│
├── outputs/
│   └── pipeline_metrics_sample.txt    ← sample run metrics
│
├── visualisations/
│   ├── chart1_category_revenue.png
│   ├── chart2_delay_vs_review.png
│   ├── chart3_state_revenue.png
│   ├── chart4_monthly_revenue_trend.png
│   ├── chart5_payment_methods.png
│   ├── chart6_delivery_delay_by_state.png
│   ├── chart7_review_distribution.png
│   └── chart8_installments_vs_order_value.png
│
└── report/
    └── olist_project_report.pdf       ← final submitted report
```

---

## Setup Instructions

### Option A — Google Colab (Recommended)

1. Download the dataset from Kaggle:  
   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

2. Upload the extracted CSV files to Google Drive:  
   `My Drive/olist_data/raw/`

3. Open `notebooks/olist_spark_pipeline.ipynb` in Google Colab.

4. Run Cell 1 to install PySpark:
   ```python
   !pip install pyspark --quiet
   ```

5. Run Cell 2 to mount Google Drive and configure paths.

6. Run all cells sequentially (Runtime → Run all).

### Option B — Local PySpark Environment

```bash
# 1. Clone the repository
git clone https://github.com/[your-username]/olist-ecommerce-spark-pipeline.git
cd olist-ecommerce-spark-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download Kaggle dataset and place CSVs in data/raw/

# 4. Update BASE_PATH in src/config.py to point to your data/raw/ folder

# 5. Run the pipeline
python -c "from src.pipeline import run_pipeline; run_pipeline()"

# Or open the notebook in Jupyter
jupyter notebook notebooks/olist_spark_pipeline.ipynb
```

---

## How to Run the Pipeline

Run the notebook cells sequentially from top to bottom.

| Cell Range | Phase                              |
|------------|------------------------------------|
| 1–8        | Environment setup and ingestion    |
| 9–15       | Data cleaning and quality checks   |
| 16–24      | Integration, alignment, transforms |
| 25–30      | ETL pipeline and Parquet output    |
| 31–40      | Spark SQL analytics                |
| 41–50      | Visualisations                     |
| 51–55      | Results, challenges, conclusion    |
| 56–59      | Repository setup and packaging     |

To re-run analytics only (after first full run):
- Start from Cell 31 — Parquet files are already written
- Cells 31–50 complete in under 2 minutes

---

## Key Findings

1. Top 10 product categories generate approximately 40–45% of total platform revenue.
2. Delivery delay is the strongest predictor of customer dissatisfaction — orders delivered 14+ days late average below 2.5 stars with over 50% poor reviews.
3. São Paulo state accounts for approximately 40% of all platform revenue.
4. Consistent year-over-year growth 2016–2018 with a clear Black Friday peak in November 2017.
5. Credit card instalment financing enables higher-value purchases — 12-instalment orders average 3–4× the value of single-payment orders.
6. A tail of high-volume sellers with average review scores below 3.0 and late delivery rates above 30% represents disproportionate platform reputational risk.

---

## Technologies Used

| Tool           | Version | Purpose                          |
|----------------|---------|----------------------------------|
| Apache Spark   | 3.5.1   | Distributed data processing      |
| PySpark        | 3.5.1   | Python Spark API                 |
| Spark SQL      | 3.5.1   | Declarative business queries     |
| Pandas         | 2.1.4   | Post-aggregation data handling   |
| Matplotlib     | 3.8.2   | Chart generation                 |
| Seaborn        | 0.13.2  | Statistical visualisations       |
| NumPy          | 1.26.4  | Numerical operations             |
| Google Colab   | —       | Execution environment            |
| Apache Parquet | —       | Curated output format (snappy)   |

---

## Business Questions Answered

| # | Question | Method |
|---|----------|--------|
| BQ1 | Top 10 product categories by revenue | GROUP BY + SUM + window annotation |
| BQ2 | Delivery delay vs customer review score | CASE WHEN bucketing + AVG |
| BQ3 | Revenue and orders by Brazilian state | GROUP BY + SUM OVER() share % |
| BQ4 | Monthly revenue and order trends 2016–2018 | Time series + 3-month rolling avg |
| BQ5 | Payment method distribution and order value | collect_set aggregation + instalment analysis |
| BQ6 | Underperforming high-volume sellers | HAVING + dual ORDER BY |

---

## Data Quality Summary

| Improvement | Detail |
|-------------|--------|
| Timestamps correctly typed | 15 columns cast with explicit format string |
| Duplicates removed | 500K+ geolocation duplicates, multi-review orders resolved |
| Column typos corrected | `product_name_lenght` → `product_name_length` (×2) |
| Category translation | Portuguese → English for ~95% of product categories |
| Invalid coordinates removed | Geographic bounding box filter for Brazil |
| Outliers filtered | 3×IQR upper bound on item prices |
| State codes standardised | F.upper() applied across customers and sellers |
| Overall data retention | >96% of original order records retained |

---

## Video Demonstration

[Video URL — YouTube / Google Drive unlisted link]

The 3–5 minute demonstration covers:
- Dataset source and business problem
- Pipeline architecture overview
- Live Spark ingestion and cleaning
- Integration join execution
- Parquet output validation
- Spark SQL analytics queries
- Visualisation walkthrough
- GitHub repository structure

---

## References

Apache Software Foundation. (2024). *Apache Spark documentation (Version 3.5)*.  
https://spark.apache.org/docs/latest/

Chambers, B., & Zaharia, M. (2018). *Spark: The definitive guide*. O'Reilly Media.

Olist. (2018). *Brazilian e-commerce public dataset by Olist* [Data set]. Kaggle.  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Karau, H., Konwinski, A., Wendell, P., & Zaharia, M. (2015).  
*Learning Spark: Lightning-fast big data analysis*. O'Reilly Media.

Hunter, J. D. (2007). Matplotlib: A 2D graphics environment.  
*Computing in Science & Engineering, 9*(3), 90–95.

Waskom, M. (2021). Seaborn: Statistical data visualization.  
*Journal of Open Source Software, 6*(60), 3021.

---

## License

This project is submitted for academic assessment.  
Dataset licensed under CC BY-NC-SA 4.0 by Olist.
