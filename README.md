
# Steps to Visualize Google Cloud Usage Fees in Looker Studio

To visualize Google Cloud usage fees in Looker Studio, follow the steps below. This guide explains how to export usage data to BigQuery using Google Cloud Billing Export and visualize it in Looker Studio.

---

## Step 1: Setting Up Google Cloud Billing Export

1. **Log in to Google Cloud Console**  
   Access [Google Cloud Console](https://console.cloud.google.com/) and select the project you are working on.

2. **Enable Billing Export**  
   - Navigate to "Billing" > "Billing Export" from the menu.
   - Click "BigQuery Export."
   - Specify the BigQuery project and dataset where the usage data will be exported.
   - Click "Enable Export" to complete the setup.

3. **Verify the Data**  
   Within a few hours (up to 24), the exported usage data will be saved in the specified BigQuery dataset. The following table is mainly created:
   - `gcp_billing_export_v1_<BillingAccountID>` (billing details by resource)

---

## Step 2: Verifying and Customizing Data in BigQuery

1. **Access BigQuery**  
   Open the [BigQuery Console](https://console.cloud.google.com/bigquery) to confirm that the billing data has been exported correctly.

2. **Create Custom Queries (Optional)**  
   Organize the usage data and create queries suitable for visualization in Looker Studio. For example:
   ```sql
   SELECT
       TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_date,
       service.description AS service_name,
       sku.description AS sku_name,
       SUM(cost) AS total_cost
   FROM
       `your_project_id.your_dataset_id.gcp_billing_export_v1_*`
   WHERE
       _TABLE_SUFFIX BETWEEN '20240101' AND '20241231'
   GROUP BY
       usage_date, service_name, sku_name
   ORDER BY
       usage_date;
   ```

---

## Step 3: Connecting Data to Looker Studio

1. **Log in to Looker Studio**  
   Visit [Looker Studio](https://lookerstudio.google.com/).

2. **Add a New Data Source**  
   - Click "Add Data Source."
   - Select "BigQuery."
   - Choose the project, dataset, and table where the usage data is stored.

3. **Check Schema**  
   Verify that fields (columns) are recognized correctly. Adjust field types if needed (e.g., set date fields as date type).

---

## Step 4: Creating and Visualizing the Report

1. **Create a New Report**  
   - Click "Create New Report."
   - Connect the previously created data source to the report.

2. **Add Graphs and Tables**  
   - Line Graph: Visualize daily usage costs.
   - Pie Chart: Display cost distribution by service.
   - Bar Graph: Compare usage costs by SKU.

3. **Add Filters and Controls**  
   - Filters: Narrow down data by specific periods or services.
   - Controls: Allow users to interactively manipulate data.

4. **Adjust Style and Formatting**  
   Ensure a clean, readable design for better accessibility.

---

## Step 5: Sharing the Report

- **Generate a Shareable Link**  
  Click "Share" > "Enable link sharing" to generate a link to the report.
- **Set Access Permissions**  
  Grant access to specific users or allow anyone with the link to view the report.

---

## Detailed Query Explanation

Below is an example query for processing Google Cloud Billing data in BigQuery, along with its interpretation.

```sql
WITH target AS(
  SELECT
    -- (SELECT rate FROM table referring to Google Sheet LIMIT 1) AS rate, -- Dollar/Yen rate * If Google Sheet is available, create a table using Google Sheet with =GOOGLEFINANCE(‘CURRENCY:USDJPY’). and reference that table,.
    150 AS rate, -- USD to JPY exchange rate (fixed value)
    6.25 AS fee_tb, -- Fee per TB (in USD)
    PARSE_DATE("%Y%m%d", @DS_START_DATE) AS start_date,
    PARSE_DATE("%Y%m%d", @DS_END_DATE) AS end_date
) 
,billing as(
  SELECT 
      service.description AS service,
      TRIM(REGEXP_EXTRACT(TRIM(COALESCE(REGEXP_EXTRACT(REPLACE(sku.description,'Long-Term','Long Term'),r'^(.*?)[(-]'), sku.description)), r'^[A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*')) AS sku_description, 
      sku.description AS original_sku_description,
      DATE(usage_start_time,'Asia/Tokyo') AS usage_date,
      project.id AS project_id,
      project.name AS project_name,
      CASE WHEN sku.description LIKE '%Replication%' THEN REGEXP_EXTRACT(resource.name, r'/datasets/([^\/]+)/') ELSE resource.name END AS resource_name, -- If sku_description is Analysis, it's a job ID; for Storage, it's a dataset name
      ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_jpy,
      MIN(DATE(DATETIME(usage_start_time,'America/Los_Angeles'))) AS usage_start_date,    
      MAX(DATE(DATETIME(usage_start_time,'America/Los_Angeles'))) AS usage_end_date
  FROM `<project_id>.all_billing_data.gcp_billing_export_resource_v1_<billing_id>` 
  CROSS JOIN target
  WHERE 
    DATE(DATETIME(usage_start_time,'Asia/Tokyo')) BETWEEN start_date AND end_date
  GROUP BY ALL
  HAVING cost_jpy > 0
)
SELECT 
  project_id,
  resource_name,
  service,
  sku_description,
  usage_date,
  cost_jpy
FROM billing;
```

### **Query Objectives**
- Aggregate BigQuery usage fees (e.g., storage, queries, replication) by date, resource, and project.
- Calculate costs in JPY considering the USD to JPY exchange rate.
- Apply credits to reflect the actual costs.

### **Key Points**
1. **Exchange Rate**:  
   The query sets `rate` as a fixed value, but it can dynamically fetch rates using external sources like Google Sheets.

2. **Period Specification**:  
   By using `@DS_START_DATE` and `@DS_END_DATE`, the query can adapt to Looker Studio's date filter.

3. **Region**:  
   Time zones are adjusted based on the region (Japan or US).

4. **Credits**:  
   Credits are applied using `UNNEST(credits)` to calculate the final cost.

---

This allows for detailed analysis of Google Cloud usage fees in Looker Studio, offering insights into project-level costs!
