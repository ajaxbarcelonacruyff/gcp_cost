WITH target AS(
SELECT 
  -- (SELECT rate FROM Googleシートを参照するテーブル LIMIT 1) AS rate, -- ドル円レート　※Googleシートが使えれば、=GOOGLEFINANCE("CURRENCY:USDJPY")を入れたGoogleシートを使ってテーブルを作成し、そのテーブルを参照する,
  150 AS rate,
  6.25 AS fee_tb, -- 1TBあたりの料金（USD）
  PARSE_DATE("%Y%m%d", @DS_START_DATE) AS start_date,
  PARSE_DATE("%Y%m%d", @DS_END_DATE) AS end_date, 
--  PARSE_DATE("%Y%m%d", "20240901") AS start_date,
--  PARSE_DATE("%Y%m%d", "20241202") AS end_date, 
),
-- クエリ費用算出用
query_jobs AS(
  -- MOLTS
  SELECT *,
  FROM `<project_id>`.`region-us`.INFORMATION_SCHEMA.JOBS	-- リージョンがUSの場合
  WHERE job_type = "QUERY" AND state = "DONE"
  AND statement_type != 'SCRIPT' -- INFORMATION_SCHEMA.JOBS にクエリを実行してクエリジョブのコストの概算を確認する場合は、SCRIPT ステートメント タイプを除外します。こうしないと、一部の値が 2 回カウントされます。SCRIPT 行には、このジョブの一部として実行されたすべての子ジョブの概要値が含まれます。
-- 別のプロジェクトでのクエリ実行も含めたい場合
--  UNION ALL
--  SELECT *,
--  FROM `<project2_id>`.`region-us`.INFORMATION_SCHEMA.JOBS	
--  WHERE job_type = "QUERY" AND state = "DONE"
--  AND statement_type != 'SCRIPT' -- INFORMATION_SCHEMA.JOBS にクエリを実行してクエリジョブのコストの概算を確認する場合は、SCRIPT ステートメント タイプを除外します。こうしないと、一部の値が 2 回カウントされます。SCRIPT 行には、このジョブの一部として実行されたすべての子ジョブの概要値が含まれます。
),
query_cost AS(
  SELECT 
  project_id,	-- クエリが実行されたプロジェクトIDなので、<project_id>か<project2_id>
  service,
  sku_description,
  COALESCE(parent_job_id, job_id) AS parent_job_id, -- job_idの親がない場合はparent_job_idがNULLとなるため、job_idをparent_job_idとする
--  parent_job_id, -- 1つのスケジュールされたクエリに対して1つ
  job_id, -- 1つのスケジュールされたクエリ内に複数のクエリがある場合は、1つ1つにJOBIDが割り当てられる
  query,
  user_email,
  (SELECT value FROM UNNEST(labels) WHERE key = "looker_studio_report_id") AS looker_studio_report_id, --  --  https://lookerstudio.google.com/reporting/xxxxxx
  (SELECT value FROM UNNEST(labels) WHERE key = "looker_studio_datasource_id") AS looker_studio_datasource_id,  --  https://lookerstudio.google.com/datasources/xxxxxx
  total_slot_ms,  -- 全期間におけるスロット（ミリ秒）
  SAFE_DIVIDE(total_slot_ms,TIMESTAMP_DIFF(end_time,start_time,MILLISECOND)) AS avg_slot_ms, -- 実行されたクエリの平均使用スロット（これの合計がtotal_slot_ms）
  SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * fee_tb *  rate AS charges_jpy, -- クエリコスト（円） ※ total_bytes_billedは1レコードに1つしか入っていないため、referenced_tablesやlabelsをUNNESTすると値がおかしくなる
  SAFE_DIVIDE(total_bytes_billed, POW(1024, 3)) AS total_gb_billed, -- クエリコスト（GB）
  SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * fee_tb AS charges_usd, -- クエリコスト（ドル）
  DATETIME(creation_time, 'America/Los_Angeles') AS creation_time,
  DATE(creation_time,'America/Los_Angeles') AS creation_date,
  referenced_tables, -- クエリコストはテーブル別では入っていないので、UNNESTすると各テーブルにtotal_bytes_billedが入るのでtotal_bytes_billedの合計値などは重複されてしまう。
  -- referenced_tables.project_id, --参照しているテーブルのプロジェクトID。複数テーブルがあるため配列　UNNESTが必要
  -- referenced_tables.dataset_id,--参照しているテーブルのデータセットID。複数テーブルがあるため配列　UNNESTが必要
  -- referenced_tables.table__id,--参照しているテーブルのテーブル名。複数テーブルがあるため配列　UNNESTが必要
  labels	-- key='datasource_id'and value ='scheduled_query'がスケジュールされたクエリ※labelsは配列
  FROM query_jobs
  CROSS JOIN target
  -- CROSS JOIN analysis
  WHERE DATE(creation_time,'America/Los_Angeles') BETWEEN start_date AND end_date -- リージョンがUSの場合はアメリカ時刻にしておく。リージョンが東京の場合は 'Asia/Tokyo'
  AND (total_bytes_billed IS NOT NULL AND total_bytes_billed != 0)
  AND DATE(creation_time,'America/Los_Angeles') BETWEEN start_date AND end_date --  請求テーブルのデータが存在する期間のみを対象とする。リージョンがUSの場合はアメリカ時刻にしておく
),
sq AS(
  SELECT DISTINCT parent_job_id,
  FROM query_cost,
  UNNEST(labels) l
  WHERE l.key = 'data_source_id' AND l.value = 'scheduled_query'
)
SELECT q.*,
IF(sq.parent_job_id IS NOT NULL, TRUE, FALSE) AS scheduled_query
FROM query_cost q LEFT JOIN sq USING(parent_job_id)

