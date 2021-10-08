select distinct batch_id
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
-- nulls in data

select distinct vendor_id
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
-- nulls in data

select distinct product_id
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
-- nulls in data

select distinct lab_id
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
-- nulls in data

select distinct tested_at
from dev_geno.lab_data_partitioned
-- where tested_at >= '2018-02-01'
order by 1 asc nulls first;
--mix of / formatted and - formatted dates with nulls

--Verify different formats of date here. Check for US vs European (month first or second)
select distinct substr(tested_at, 5, 1)
from dev_geno.lab_data_partitioned
;

select distinct substr(tested_at, 1, 4)
from dev_geno.lab_data_partitioned
order by 1; -- this is year

select distinct substr(tested_at, 6, 2)
from dev_geno.lab_data_partitioned
order by 1; -- this is month

select distinct substr(tested_at, 9, 2)
from dev_geno.lab_data_partitioned
order by 1; --this appears to be day


select *
from dev_geno.lab_data_partitioned
where tested_at is null;

select distinct expires_at
from dev_geno.lab_data_partitioned
-- where tested_at >= '2019-02-01'
order by 1 asc nulls first;
--mix of / formatted and - formatted dates with nulls

select *
from dev_geno.lab_data_partitioned
where tested_at >= expires_at
;

select distinct thc
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
-- negative values? what does that mean? take abs? invalid?

select distinct thca
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;

select distinct cbd
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;

select distinct cbda
from dev_geno.lab_data_partitioned
order by 1 asc nulls first;
--all potency values have negatives, how to handle?