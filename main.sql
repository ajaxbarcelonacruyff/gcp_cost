WITH target AS(
SELECT 
  -- (SELECT rate FROM Googleシートを参照するテーブル LIMIT 1) AS rate, -- ドル円レート　※Googleシートが使えれば、=GOOGLEFINANCE("CURRENCY:USDJPY")を入れたGoogleシートを使ってテーブルを作成し、そのテーブルを参照する,
  150 AS rate,
  6.25 AS fee_tb, -- 1TBあたりの料金（USD）
  PARSE_DATE("%Y%m%d", @DS_START_DATE) AS start_date,
  PARSE_DATE("%Y%m%d", @DS_END_DATE) AS end_date, 
--  PARSE_DATE("%Y%m%d", "20240901") AS start_date,
--  PARSE_DATE("%Y%m%d", "20241202") AS end_date, 
) 
-- ストレージ費用など（クエリコストも含まれる（sku.description = 'Analysis'）がデータセット単位で抽出できないため今回は除外する
,billing as(
SELECT 
    service.description AS service-- BigQuery
    ,TRIM(REGEXP_EXTRACT(TRIM(COALESCE(REGEXP_EXTRACT(REPLACE(sku.description,'Long-Term','Long Term'),r'^(.*?)[(-]'), sku.description)) , r'^[A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*')) AS sku_description
    ,sku.description AS original_sku_description -- Long Term Logical Storage
    ,DATE(usage_start_time,'America/Los_Angeles') AS usage_date  -- ,usage_start_time, usage_end_time -- 1時間単位
    ,project.id AS project_id
    ,project.name AS project_name
    ,CASE WHEN sku.description LIKE '%Replication%' THEN REGEXP_EXTRACT(resource.name, r'/datasets/([^\/]+)/') ELSE resource.name END AS resource_name
 
    ,ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2)  AS cost_jpy,
    MIN(DATE(DATETIME(usage_start_time,'America/Los_Angeles'))) AS usage_start_date,    
    MAX(DATE(DATETIME(usage_start_time,'America/Los_Angeles'))) AS usage_end_date,    
-- FROM `<project_id>.all_billing_data.gcp_billing_export_resource_v1_<billing_id>` 
CROSS JOIN target
WHERE 
-- service.description LIKE 'BigQuery%'
--	AND sku.description != 'Analysis'
	DATE(DATETIME(usage_start_time,'Asia/Tokyo')) BETWEEN start_date AND end_date -- リージョンが東京の場合
	-- AND DATE(DATETIME(usage_start_time,'America/Los_Angeles')) BETWEEN start_date AND end_date -- リージョンがUSの場合
GROUP BY ALL
HAVING cost_jpy >0
)
SELECT 
  project_id,
  resource_name , -- Active Logical Storageのときはデータセット名、AnalysisのときはジョブID
  service, -- BigQuery, BigQuery Storage API
  sku_description, -- Active Logical Storage, Analysis
  usage_date, -- 使用日
  cost_jpy, -- 費用（円）
  FROM billing 
