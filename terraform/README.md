# 🧱 Databricks ETL Pipeline – Terraform Infrastructure

This Terraform setup provisions a full **Azure + Databricks** environment for the Databricks Medallion ETL Pipeline project. The deployment is divided into two independent stages for modularity and clarity.

---

## 🚀 Overview

| Stage | Folder             | Description                                                                                                |
| ----- | ------------------ | ---------------------------------------------------------------------------------------------------------- |
| **1** | `1_azure_infra/`   | Deploys Azure infrastructure (Resource Group, Storage, Databricks Workspace, Access Connector, Key Vault). |
| **2** | `2_databricks_uc/` | Configures Databricks Unity Catalog and securely stores the Databricks Token in Key Vault.                 |

---

## 🧩 Folder Structure

```bash
terraform/
├── README.md
│
├── 1_azure_infra/
│   ├── main.tf
│   ├── outputs.tf
│   ├── providers.tf
│   └── variables.tf
│
└── 2_databricks_uc/
    ├── databricks.tf
│   ├── outputs.tf
│   ├── providers.tf
│   └── variables.tf
```

---

## ⚙️ Prerequisites

* [Terraform ≥ 1.2](https://developer.hashicorp.com/terraform/downloads)
* Azure CLI logged in (`az login`)
* Databricks account linked to Azure subscription

---

## 🏗️ 1️⃣ Deploy Azure Infrastructure

Navigate to the infrastructure folder and deploy all Azure resources:

```bash
cd terraform/1_azure_infra
terraform init
terraform apply
```

This step will create:

* Resource Group
* ADLS Gen2 Storage (containers: source, bronze, silver, gold, metastore)
* Databricks Workspace
* Databricks Access Connector (with permissions to ADLS)
* Azure Key Vault (empty, ready to store Databricks token)

> **Note:** No Databricks token is required at this stage.

---

## 🔑 2️⃣ Generate Databricks Token

After the Databricks workspace is deployed:

1. Open the Databricks UI → **User Settings → Developer → Access Tokens**
2. Click **Generate New Token**
3. Copy the token (e.g., `dapiXXXXXXXXXXXX`)

---

## 🧠 3️⃣ Deploy Unity Catalog & Store Token

Once the token is generated, move to the Unity Catalog deployment folder:

```bash
cd ../2_databricks_uc
terraform init
terraform apply -var "databricks_token=dapiXXXXXXXXXXXX"
```

This stage will:

* Save your Databricks token securely into Azure Key Vault
* Automatically retrieve it to authenticate the Databricks provider
* Create:

  * Unity Catalog Metastore
  * Workspace binding
  * Main catalog
  * Bronze, Silver, and Gold schemas

---

## 🔒 Security Notes

* The Databricks token is stored securely in **Azure Key Vault**.
* Terraform automatically assigns your user the **Key Vault Secrets Officer** role, allowing you to manage secrets.
* No token value is written to local `.tfstate` or terminal logs.

---

## 🧹 Cleanup

To destroy all resources:

```bash
cd terraform/2_databricks_uc
terraform destroy

cd ../1_azure_infra
terraform destroy
```

> Always destroy the Unity Catalog resources first to ensure proper dependency handling.

---

## 📘 Additional Notes

* Remote state from `1_azure_infra` is automatically read by `2_databricks_uc` using:

  ```terraform
  data "terraform_remote_state" "azure" {
    backend = "local"
    config = {
      path = "../1_azure_infra/terraform.tfstate"
    }
  }
  ```
* You can adapt this to a remote backend (e.g., Azure Blob Storage) for team deployments.

---

## ✅ Summary

After both stages:

* Azure infrastructure and Databricks workspace are deployed.
* Databricks token is securely stored in Key Vault.
* Unity Catalog and schemas (Bronze, Silver, Gold) are fully configured and ready for ETL pipeline use.
