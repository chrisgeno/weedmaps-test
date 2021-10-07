CREATE TABLE lab_data_partitioned
    WITH (
        format = 'PARQUET',
        external_location = 's3://salsify-geno-dev/queryresults/',
        partitioned_by = ARRAY ['state']
        --bucketed_by = ARRAY['vendor_id'],
        --bucket_count = 3)
        )
AS
SELECT batch_id,
       vendor_id,
       product_id,
       lab_id,
       tested_at,
       expires_at,
       thc,
       thca,
       cbd,
       cbda,
       state
FROM lab_data
where state <= 'OR'
;

insert into lab_data_partitioned
SELECT batch_id,
       vendor_id,
       product_id,
       lab_id,
       tested_at,
       expires_at,
       thc,
       thca,
       cbd,
       cbda,
       state
FROM lab_data
where state > 'OR'
;

drop table dev_geno.lab_data_partitioned_normalized;
create table dev_geno.lab_data_partitioned_normalized
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY [ 'state' ]
        ) as
select batch_id,
       vendor_id,
       product_id,
       lab_id,
       cast(replace(tested_at, '/', '-') as date)  as tested_at,
       cast(replace(expires_at, '/', '-') as date) as expires_at,
       thc,
       thca,
       cbd,
       cbda,
       ldp.state                                   as old_state,
       sn.state
from dev_geno.lab_data_partitioned ldp
         left join dev_geno.statecodes_normalized sn
                   on lower(regexp_replace(ldp.state, '[^a-zA-Z]')) = sn.normal_forms[1] or
                      lower(regexp_replace(ldp.state, '[^a-zA-Z]')) = sn.normal_forms[2] or
                      lower(regexp_replace(ldp.state, '[^a-zA-Z]')) = sn.normal_forms[3]
;

select count(*)
from dev_geno.lab_data_partitioned;
select count(*)
from dev_geno.lab_data_partitioned_normalized;

select distinct old_state, state
from dev_geno.lab_data_partitioned_normalized;
