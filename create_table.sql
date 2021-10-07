CREATE EXTERNAL TABLE IF NOT EXISTS dev_geno.`lab_data` (
  `batch_id` string,
  `vendor_id` string,
  `product_id` string,
  `lab_id` string,
  `state` string,
  `tested_at` string,
  `expires_at` string,
  `thc` float,
  `thca` float,
  `cbd` float,
  `cbda` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://salsify-geno-dev/dev_geno/'
TBLPROPERTIES ('has_encrypted_data'='false');