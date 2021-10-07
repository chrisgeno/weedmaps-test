
/*
1. At least 99.5% of products will be tested every week.
2. Every vendor has 3 labs that they use at different times.
3. The data takes place over a 2 year window.
4. No products or vendors or labs churn during those 2 years.
5. Vendors and Labs are always located in the same state.
6. The output format is always json and each line is a complete entry
 */

-- 1. Which 5 vendors have the most products?
with vendor_product_counts as (
    select vendor_id,
           count(distinct product_id) as product_count,
           dense_rank() over (order by count(distinct product_id) desc) as rank
    from dev_geno.lab_data_partitioned_normalized
    where vendor_id is not null
      and product_id is not null
    group by 1
    order by 2 desc
)
select *
from vendor_product_counts
where rank <= 5
order by 2 desc

-- 2. Which 5 vendors have the fewest products?
-- simply changing our rank function and order by to use asc instead
with vendor_product_counts as (
    select vendor_id,
           count(distinct product_id) as product_count,
           dense_rank() over (order by count(distinct product_id) asc) as rank
    from dev_geno.lab_data_partitioned_normalized
    where vendor_id is not null
      and product_id is not null
    group by 1
    order by 2 asc
)
select *
from vendor_product_counts
where rank <= 5
order by 2 asc;

-- 3. Which 5 products have the highest potency?
-- Potency is defined as thc + thca + cbd + cbda
-- Assuming that potency is the value from the latest tested_at date for a product
select product_id,
       count(distinct tested_at),
       min(tested_at),
       max(tested_at),
       min(expires_at),
       max(expires_at),
       min(thc),
       max(thc),
       min(thca),
       max(thca),
       min(cbd),
       max(cbd),
       min(cbda),
       max(cbda)
from dev_geno.lab_data_partitioned_normalized
group by 1 order by 7 asc;

select product_id, tested_at, expires_at
from dev_geno.lab_data_partitioned_normalized
where tested_at > expires_at;

with latest_product_test as (
    select product_id,
           max(tested_at) as latest_tested_at
    from dev_geno.lab_data_partitioned_normalized
    group by 1
)
select ldpn.product_id,
       thc + thca + cbd + cbda as total_potency,
       thc,
       thca,
       cbd,
       cbda,
       dense_rank() over (order by thc + thca + cbd + cbda desc) as rank
from dev_geno.lab_data_partitioned_normalized ldpn
inner join latest_product_test lpt on ldpn.product_id = lpt.product_id and
                                      ldpn.tested_at = lpt.latest_tested_at
order by 2 desc
;


select distinct product_id,
       thc + thca + cbd + cbda as total_potency,
                tested_at,
                expires_at
from dev_geno.lab_data_partitioned_normalized
where product_id = 'bcddb81d-2500-4d6c-90cd-4c1da3c859c1'
order by 3 desc;

-- 4. Which 5 products have the lowest potency?
with latest_product_test as (
    select product_id,
           max(tested_at) as latest_tested_at
    from dev_geno.lab_data_partitioned_normalized
    group by 1
)
select ldpn.product_id,
       thc + thca + cbd + cbda as total_potency,
       thc,
       thca,
       cbd,
       cbda,
       dense_rank() over (order by thc + thca + cbd + cbda asc) as rank
from dev_geno.lab_data_partitioned_normalized ldpn
inner join latest_product_test lpt on ldpn.product_id = lpt.product_id and
                                      ldpn.tested_at = lpt.latest_tested_at
order by 2 asc
;

select distinct product_id,
       thc + thca + cbd + cbda as total_potency,
                tested_at,
                expires_at
from dev_geno.lab_data_partitioned_normalized
where product_id = '7cfc3c4a-9b80-4362-bc37-1d86442ecaef'
order by 3 desc;


-- 5. Which 5 labs have the highest accuracy?
--bad data, nulls for a lab, impossible values for thc, thca, cbd, cbda (negatives, outside 3 standard deviations?


-- 6. Which 5 labs have the lowest accuracy?


-- 7. Which 5 states have the most products and how many?
select state,
       count(distinct product_id) as num_products,
       dense_rank() over (order by count(distinct product_id) desc) as rank
from dev_geno.lab_data_partitioned_normalized
group by 1 order by 2 desc;



-- 8. Which 5 states have the fewest products and how few?
select state,
       count(distinct product_id) as num_products,
       dense_rank() over (order by count(distinct product_id) asc) as rank
from dev_geno.lab_data_partitioned_normalized
group by 1 order by 2 asc;


-- 9. How many tests are performed each day of the week?
select tested_at,
       day_of_week(tested_at),
       count(distinct batch_id) as batch_tested
from dev_geno.lab_data_partitioned_normalized
where tested_at >= cast('2019-02-01' as date) and
      state = 'California'
group by 1,2 order by 1;

--Make sure batches can only have one product_id
select batch_id,
       count(distinct product_id)
from dev_geno.lab_data_partitioned_normalized
group by 1 order by 2 desc;

--Validate that products always have multiple batches
select product_id,
       count(distinct batch_id)
from dev_geno.lab_data_partitioned_normalized
group by 1 order by 2 desc;