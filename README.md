# 醫療掛號資料分析系統（Hospital Data Analytics Pipeline）
## 目標
本專案模擬醫院資訊系統（HIS）的資料整合與分析流程，透過 **資料倉儲（DWH）建置、ETL 自動化、dbt 建模、Tableau 視覺化**，最終建立一個可即時觀察每日就診趨勢與科別分佈的數據平台。

---

## 架構邏輯總覽

### 1.Data Source：HIS 系統（MySQL）
- Python 建立模擬表
- 以 Faker 產生假資料模擬真實場景

---

### 2.Data Ingestion：資料同步（Airbyte）
- Airbyte 負責自動將 **MySQL → PostgreSQL** 的資料同步
- 支援 **CDC（Change Data Capture）**
- 每個來源表格對應 PostgreSQL 的「public schema」。


---

### 3.dbt（資料建模）
資料分為三層架構：

| 層級 | 命名規則 | 內容說明 |
|------|------------|------------|
| **staging 層 (stg_)** | `stg_patients`, `stg_appointments`, `stg_lab_results`, `stg_diagnosis.sql` | 清洗與標準化欄位 |
| **intermediate 層 (int_)** | `int_patient_visits` | 整合多表資訊 |
| **mart 層 (marts_)** | `daily_kpis`, `patients_with_kpis`, `daily_department_kpis` | 產出分析用 KPI |

主要模型：
- models/
    - staging/
        - stg_patients.sql 清洗病患資料
        - stg_appointments.sql 掛號資料（包含日期、科別、醫師）
        - stg_diagnosis.sql 就診資料與病患代號
        - stg_lab_results.sql 檢驗資料與結果分類
    - intermediate/
        - int_patient_visits.sql 掛號、就診、檢驗資料整合成病患層級資料
    - marts/
        - daily_kpis.sql 計算每日關鍵指標（平均等待時間、異常檢驗率、掛號總量）
        - patients_with_kpis.sql 病患層級的 KPI 整合表
        - daily_department_kpis.sql 各科每日掛號比例與總量分析


