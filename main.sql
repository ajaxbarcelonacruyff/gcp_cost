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
