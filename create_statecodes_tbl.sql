CREATE EXTERNAL TABLE IF NOT EXISTS dev_geno.`statecodes` (
  `state` string,
  `abbreviation` string,
  `code` string,
  `capital` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://salsify-geno-dev/statecodes/'
TBLPROPERTIES ("skip.header.line.count"="1",
'has_encrypted_data'='false');

CREATE table dev_geno.statecodes_normalized as
select state,
	abbreviation,
	code,
	ARRAY[lower(regexp_replace(state, '[^a-zA-Z]')), lower(regexp_replace(abbreviation, '[^a-zA-Z]')), lower(regexp_replace(code, '[^a-zA-Z]'))] as normal_forms,
	capital
from statecodes