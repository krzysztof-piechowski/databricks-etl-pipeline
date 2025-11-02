# Databricks Medallion ETL

End-to-end ETL pipeline built on **Azure Databricks** following the **Medallion Architecture (Bronze–Silver–Gold)** pattern.
The project demonstrates a modular data ingestion and transformation workflow using **Delta Lake**, **Auto Loader**, **Azure Data Lake Storage**, and **Unity Catalog** for governance.
Additionally, the entire infrastructure is fully **automated and reproducible** through **Terraform**, enabling Infrastructure as Code (IaC) deployment on Azure.

---

## 🏗️ Architecture Overview

![Pipeline Architecture](docs/architecture.png)

The pipeline follows the Medallion design:

* **Bronze layer:** Raw data ingestion using Auto Loader (cloudFiles) in a batch-style trigger-once mode.
* **Silver layer:** Data cleaning, standardization, and schema enforcement.
* **Gold layer:** Aggregated and business-ready data for analytics or reporting.

Each layer is stored in a separate **ADLS container** to ensure physical separation of data and clear data lifecycle management.

---

## 🗂️ Unity Catalog Integration

This project uses **Azure Databricks Unity Catalog** for centralized governance, metadata, and schema management, while maintaining separate **Azure Data Lake Storage (ADLS)** containers for each Medallion layer.

* **Data Storage:**
  Each layer (Bronze, Silver, Gold) resides in a dedicated ADLS container:

  * `abfss://bronze@storageaccpiechk.dfs.core.windows.net/`
  * `abfss://silver@storageaccpiechk.dfs.core.windows.net/`
  * `abfss://gold@storageaccpiechk.dfs.core.windows.net/`

* **Unity Catalog Role:**
  Unity Catalog manages metadata and schemas for Delta tables that reference these external locations.
  It provides access control, lineage, and governance without physically storing the data.

> This architecture separates data storage from metadata governance, ensuring flexibility and security across layers.

---

## 📂 Notebook Workflow

The ETL process is divided into parameterized Databricks notebooks, executed sequentially as part of an orchestrated job.

| Notebook                                   | Description                                                                                                                          |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `0_parameters`                             | Defines dataset parameters (`orders`, `customers`, `products`) used to dynamically control the Bronze ingestion loop.                |
| `1_Bronze_Layer`                           | Reads raw data from the source container and writes to Bronze layer. Executed in a loop for each dataset defined in `parameters.py`. |
| `2_Silver_Customers`                       | Cleans and transforms the Bronze `customers` dataset.                                                                                |
| `3_Silver_Orders`                          | Cleans and standardizes the Bronze `orders` dataset with partitioning and ZORDER optimization.                                       |
| `4_Silver_Products`                        | Cleans and transforms the Bronze `products` dataset.                                                                                 |
| `5_Checkpoints And Batches Table Creation` | Manages Delta tables for tracking ingestion batches and checkpointing.                                                               |
| `6_Gold_Customers`                         | Builds SCD Type 2 dimension table (`DimCustomers`) with historical tracking and versioning.                                          |
| `7_Gold_Products`                          | Builds SCD Type 2 dimension table (`DimProducts`) with historical tracking and versioning.                                           |
| `8_Fact_Orders`                            | Builds the main fact table by joining dimensional tables, applying partitioning and optimization.                                    |

---

## ⚙️ Technologies Used

* **Azure Databricks** (PySpark, Delta Lake)
* **Azure Data Lake Storage (ADLS Gen2)**
* **Databricks Auto Loader**
* **Unity Catalog** (governance & metadata)
* **Terraform** (Infrastructure as Code)
* **Python / Spark Structured Streaming**
* **Medallion Architecture (Bronze–Silver–Gold)**
* **Delta Change Data Feed (CDF)** for incremental processing

---

## 🚀 Pipeline Workflow

1. **Parameter definition:**
   Dataset names and configurations for Bronze Layer Auto Loader are defined in `parameters.py`.

2. **Bronze ingestion:**
   Data is ingested from ADLS containers via Databricks Auto Loader:

   ```python
   df = spark.readStream.format("cloudFiles") \
       .option("cloudFiles.format", "parquet") \
       .option("cloudFiles.schemaLocation",f"abfss://bronze@storageaccpiechk.dfs.core.windows.net/checkpoint_{source_file_name}") \
       .load(f"abfss://source@storageaccount.dfs.core.windows.net/{source_file_name}")
   ```

   The Bronze notebook is executed in a loop for each dataset defined in parameters.

3. **Silver transformation:**
   Data cleaning, normalization, and schema enforcement for each entity with partitioning and optimization applied.

4. **Gold aggregation:**
   Curated data aggregation, creation of fact and dimension tables, and incremental updates via Delta CDF.

---

## 🧱 Advanced Features

* **SCD Type 2 Implementation:**
  Gold dimension tables (`DimCustomers`, `DimProducts`) maintain full change history using the SCD Type 2 pattern implemented with Delta Change Data Feed (CDF) and merge-based upserts.

* **Physical Partitioning and Optimization:**
  Silver and Gold fact tables are physically partitioned by time attributes (`year`, `month`) and optimized with ZORDER indexing.
  Additional tuning includes:

  * `OptimizeWrite` and `AutoCompact` enabled
  * Dynamic partition pruning
  * Broadcast joins for dimensional lookups
  * Target Delta file size configuration

---

## 🧮 Gold Fact Table Logic

The **Gold Fact Table (`FactOrders`)** represents the central transactional dataset in the Medallion pipeline, built incrementally using **Delta Change Data Feed (CDF)** from the Silver layer.
This stage integrates multiple advanced mechanisms ensuring **data reliability**, **idempotency**, and **governance**.

---

### 🔁 Incremental Load via Delta CDF

* Detects the latest processed version of the Silver `orders` Delta table.
* Reads only **new changes** using CDF.
* Tracks progress using two Delta logs:

  * `gold.checkpoint_log` – stores the last processed Silver version
  * `gold.batch_log` – records batch ID, execution metadata, row counts, and errors

```python
current_version = get_current_silver_version()
last_version = get_last_processed_version()
```

Only unprocessed versions are included in the next load.

---

### 🧩 SCD-Aware Dimensional Joins

Before inserting into the fact table, new rows are joined with current dimension tables to resolve surrogate keys:

```python
dim_products = spark.table(dim_products_table).filter("is_current = true")
dim_customers = spark.table(dim_customers_table).filter("is_current = true")
```

This maintains **referential integrity** between the fact and dimensions, even as dimension tables evolve via **SCD Type 2** updates.

---

### ⚠️ Missing Dimension Handling

Records referencing nonexistent dimension keys are isolated to a separate Delta table:

```
abfss://gold@storageaccpiechk.dfs.core.windows.net/FactOrders_MissingDims/
```

These are excluded from production tables until corresponding dimension records exist, ensuring **pipeline continuity without integrity loss**.

---

### 🧱 Staging and Idempotent MERGE

All new data is first written to a temporary staging table, ensuring deterministic, idempotent inserts:

```python
staging_path = f"{staging_root}/batch_{batch_id}/"
transformed.write.format("delta").mode("overwrite").save(staging_path)
```

Then merged into the main table:

```python
delta_fact.alias("t").merge(
    F.broadcast(staging_df).alias("s"),
    "t.order_id = s.order_id AND t.year = s.year AND t.month = s.month"
).whenNotMatchedInsertAll().execute()
```

This guarantees that **reprocessing or retries never duplicate data**.

---

### 🧠 Resilience & Retry Logic

* Automatic retries with exponential backoff (`MAX_RETRIES`, `RETRY_BACKOFF_SEC`)
* Batch and checkpoint states updated only upon successful completion
* All exceptions logged to Delta logs and re-raised for observability

Ensures **fault-tolerant execution** and full **operational traceability**.

---

### ⚙️ Performance Optimizations

* Partitioned by `year`, `month`
* **ZORDERed** by `customer_sk`, `product_sk`
* `OptimizeWrite` and `AutoCompact` enabled
* Broadcast joins for small dimensional lookups

---

### 📋 Example Logs

Execution lineage and checkpoints can be audited directly via SQL:

```sql
SELECT * FROM databricks_cata.gold.checkpoint_log;
SELECT * FROM databricks_cata.gold.batch_log;
```

---

## 🧰 Infrastructure as Code (Terraform)

The full Azure infrastructure required for this project is provisioned using **Terraform**. The setup is divided into two modular stages:

| Stage | Folder                       | Description                                                                                                        |
| ----- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **1** | `terraform/1_azure_infra/`   | Creates Azure infrastructure: Resource Group, ADLS Storage, Databricks Workspace, Access Connector, and Key Vault. |
| **2** | `terraform/2_databricks_uc/` | Configures Databricks Unity Catalog and securely stores the Databricks token in Key Vault.                         |

Key Terraform Features:

* Automated provisioning of all Azure and Databricks resources
* Secure token storage in **Azure Key Vault**
* Modular separation between infrastructure and Unity Catalog setup
* Full reusability and reproducibility for CI/CD pipelines

> Terraform enables one-command deployment of the complete data platform, ensuring consistent and secure infrastructure setup across environments.

---

## 🧩 Setup & Deployment

1. Clone this repository:

   ```bash
   git clone https://github.com/krzysztof-piechowski/databricks-etl-pipeline.git
   ```

2. Provision infrastructure using Terraform (see `/terraform/README.md` for detailed steps):

   ```bash
   cd terraform/1_azure_infra
   terraform init
   terraform apply

   cd ../2_databricks_uc
   terraform init
   terraform apply -var "databricks_token=dapiXXXXXXXXXXXX"
   ```

3. Upload `.py` files or notebooks to Azure Databricks workspace.

4. Run the notebooks sequentially or as part of a Databricks Job pipeline.

---

## 📈 Future Improvements

* Integrate **Delta Live Tables** or Databricks Workflows for orchestration.
* Add data quality checks using **Great Expectations** or **Deequ**.
* Implement CI/CD deployment with **Azure DevOps** or **GitHub Actions**.
* Introduce monitoring and alerting via **Azure Monitor**.

---

## 📚 Author

**Krzysztof Piechowski**  
Data Engineer | Azure | Databricks | ETL | Delta Lake  
[LinkedIn](https://linkedin.com/in/krzysztof-piechowski) • [GitHub](https://github.com/krzysztof-piechowski)  

---

> This project showcases a modern ETL pipeline using Databricks, ADLS, and Unity Catalog in the Medallion Architecture pattern, with SCD Type 2, Delta Lake optimizations, and fully automated infrastructure deployment through Terraform.
