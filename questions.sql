/*
1. At least 99.5% of products will be tested every week.
2. Every vendor has 3 labs that they use at different times.
3. The data takes place over a 2 year window.
4. No products or vendors or labs churn during those 2 years.
5. Vendors and Labs are always located in the same state.
6. The output format is always json and each line is a complete entry
 */

-- 1. Which 5 vendors have the most products? (Run Time 8.58 seconds)
with vendor_product_counts as (
    select vendor_id,
           count(distinct product_id)                                   as product_count,
           dense_rank() over (order by count(distinct product_id) desc) as rank
    from dev_geno.lab_data_partitioned_normalized
    where vendor_id is not null
      and product_id is not null
      and thc >= 0
      and thca >= 0
      and cbd >= 0
      and cbda >= 0
    group by 1
    order by 2 desc
)
select *
from vendor_product_counts
where rank <= 5
order by 2 desc
;

-- 2. Which 5 vendors have the fewest products? (Run time 8.076 seconds)
-- simply changing our rank function and order by to use asc instead
with vendor_product_counts as (
    select vendor_id,
           count(distinct product_id)                                  as product_count,
           dense_rank() over (order by count(distinct product_id) asc) as rank
    from dev_geno.lab_data_partitioned_normalized
    where vendor_id is not null
      and product_id is not null
      and thc >= 0
      and thca >= 0
      and cbd >= 0
      and cbda >= 0
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
group by 1
order by 7 asc;

select product_id, tested_at, expires_at
from dev_geno.lab_data_partitioned_normalized
where tested_at > expires_at;

with latest_product_test as ( --runtime 12.982 seconds
    select product_id,
           max(tested_at) as latest_tested_at
    from dev_geno.lab_data_partitioned_normalized
    group by 1
),
     potency as (
         select ldpn.product_id,
                thc + thca + cbd + cbda                                   as total_potency,
                thc,
                thca,
                cbd,
                cbda,
                dense_rank() over (order by thc + thca + cbd + cbda desc) as rank
         from dev_geno.lab_data_partitioned_normalized ldpn
                  inner join latest_product_test lpt on ldpn.product_id = lpt.product_id and
                                                        ldpn.tested_at = lpt.latest_tested_at
         where ldpn.product_id is not null
           and thc >= 0
           and thca >= 0
           and cbd >= 0
           and cbda >= 0
         order by 2 desc
     )
select *
from potency
where rank <= 5
;

-- sanity check all data for top 5 product_id's
select *
from dev_geno.lab_data_partitioned_normalized
--where product_id = 'bcddb81d-2500-4d6c-90cd-4c1da3c859c1'
-- where product_id = '7226a48c-01b4-43db-91dc-18350ce873ef'
-- where product_id = '06f9f30d-815f-4b79-b200-5b1e676babf6'
-- where product_id = '7491d0f4-951c-4b48-8578-430aeefe8bbe'
where product_id = 'a8d3ead3-5571-4114-b9b2-0dc368576781'
order by tested_at desc
;

-- 4. Which 5 products have the lowest potency? --runtime 12.757 seconds
with latest_product_test as (
    select product_id,
           max(tested_at) as latest_tested_at
    from dev_geno.lab_data_partitioned_normalized
    group by 1
),
     potency as (
         select ldpn.product_id,
                thc + thca + cbd + cbda                                  as total_potency,
                thc,
                thca,
                cbd,
                cbda,
                dense_rank() over (order by thc + thca + cbd + cbda asc) as rank
         from dev_geno.lab_data_partitioned_normalized ldpn
                  inner join latest_product_test lpt on ldpn.product_id = lpt.product_id and
                                                        ldpn.tested_at = lpt.latest_tested_at
         where ldpn.product_id is not null
           and thc >= 0
           and thca >= 0
           and cbd >= 0
           and cbda >= 0
         order by 2 asc
     )
select *
from potency
where rank <= 5
;

select distinct product_id,
                thc + thca + cbd + cbda as total_potency,
                tested_at,
                expires_at
from dev_geno.lab_data_partitioned_normalized
where product_id = '7cfc3c4a-9b80-4362-bc37-1d86442ecaef'
order by 3 desc;


-- 5. Which 5 labs have the highest accuracy?  (runtime 8.521s)
--inaccurate reading would be::: bad data, nulls for a lab, 
--impossible values for thc, thca, cbd, cbda (negatives, ?? values outside 3 standard deviations??
with vendor_accuracy as
         (
             select lab_id,
                    (thc >= 0 and thca >= 0 and cbd >= 0 and cbda >= 0)                         as is_potency_valid,
                    (batch_id is not null and vendor_id is not null and product_id is not null) as is_id_valid,
                    (state = old_state)                                                         as is_state_accurate
             from dev_geno.lab_data_partitioned_normalized
             where lab_id is not null
         ),
     validity_totals as (
         select lab_id,
                sum(
                        case
                            when is_potency_valid and is_id_valid and is_state_accurate then 1
                            else 0
                            end
                    ) as num_valid,
                sum(
                        case
                            when not is_potency_valid or not is_id_valid or not is_state_accurate then 1
                            else 0
                            end
                    ) as num_invalid
         from vendor_accuracy
         group by 1
         order by 1
     ),
     accuracy as (
         select lab_id,
                num_valid,
                num_invalid,
                1.00 * num_valid / (num_valid + num_invalid) * 100.00 as pct_accurate
         from validity_totals
     )
     select lab_id,
            num_valid,
            num_invalid,
            pct_accurate,
            dense_rank() over (order by pct_accurate desc) as rank
from accuracy
order by dense_rank() over (order by pct_accurate desc)
;

--------------ITERATION 2, state not considered--------------------

with vendor_accuracy as
         (
             select lab_id,
                    (thc >= 0 and thca >= 0 and cbd >= 0 and cbda >= 0)                         as is_potency_valid,
                    (batch_id is not null and vendor_id is not null and product_id is not null) as is_id_valid
             from dev_geno.lab_data_partitioned_normalized
             where lab_id is not null
         ),
     validity_totals as (
         select lab_id,
                sum(
                        case
                            when is_potency_valid and is_id_valid then 1
                            else 0
                            end
                    ) as num_valid,
                sum(
                        case
                            when not is_potency_valid or not is_id_valid then 1
                            else 0
                            end
                    ) as num_invalid
         from vendor_accuracy
         group by 1
         order by 1
     ),
     accuracy as (
         select lab_id,
                num_valid,
                num_invalid,
                1.00 * num_valid / (num_valid + num_invalid) * 100.00 as pct_accurate
         from validity_totals
     )
     select lab_id,
            num_valid,
            num_invalid,
            pct_accurate,
            dense_rank() over (order by pct_accurate desc) as rank
from accuracy
order by dense_rank() over (order by pct_accurate desc)
;

-- 6. Which 5 labs have the lowest accuracy?
with vendor_accuracy as
         (
             select lab_id,
                    (thc >= 0 and thca >= 0 and cbd >= 0 and cbda >= 0)                         as is_potency_valid,
                    (batch_id is not null and vendor_id is not null and product_id is not null) as is_id_valid,
                    (state = old_state)                                                         as is_state_accurate
             from dev_geno.lab_data_partitioned_normalized
             where lab_id is not null
         ),
     validity_totals as (
         select lab_id,
                sum(
                        case
                            when is_potency_valid and is_id_valid and is_state_accurate then 1
                            else 0
                            end
                    ) as num_valid,
                sum(
                        case
                            when not is_potency_valid or not is_id_valid or not is_state_accurate then 1
                            else 0
                            end
                    ) as num_invalid
         from vendor_accuracy
         group by 1
         order by 1
     ),
     accuracy as (
         select lab_id,
                num_valid,
                num_invalid,
                1.00 * num_valid / (num_valid + num_invalid) * 100.00 as pct_accurate
         from validity_totals
     )
     select lab_id,
            num_valid,
            num_invalid,
            pct_accurate,
            dense_rank() over (order by pct_accurate asc) as rank
from accuracy
order by dense_rank() over (order by pct_accurate asc)
;

select *
from dev_geno.lab_data_partitioned_normalized
where lab_id = '37e2ca91-3157-451a-8f09-d3ffb0853e69'  --lab with 0 percent accuracy

--did accurate state naming conventions change over time?
select distinct year(tested_at), old_state
from dev_geno.lab_data_partitioned_normalized
order by 1,2 asc
-- No, Codes are mixed in 2018



-- 7. Which 5 states have the most products and how many? (runtime 4.579s)
select state,
       count(distinct product_id)                                   as num_products,
       dense_rank() over (order by count(distinct product_id) desc) as rank
from dev_geno.lab_data_partitioned_normalized
where product_id is not null
group by 1
order by 2 desc;


-- 8. Which 5 states have the fewest products and how few? (runtime 5.499s)
select state,
       count(distinct product_id)                                  as num_products,
       dense_rank() over (order by count(distinct product_id) asc) as rank
from dev_geno.lab_data_partitioned_normalized
where product_id is not null
group by 1
order by 2 asc;


-- 9. How many tests are performed each day of the week?
select batch_id, count(distinct tested_at)
from dev_geno.lab_data_partitioned_normalized
where tested_at >= cast('2019-01-01' as date)
  and state = 'California'
group by 1
order by 2 desc;

select *
from dev_geno.lab_data_partitioned_normalized
where day_of_week(tested_at) is null;

with batches_tested_per_day as (
    select tested_at,
           day_of_week(tested_at)       as day_of_week,
           date_format(tested_at, '%a') as day_of_week_str,
           count(distinct batch_id)     as batch_tested
    from dev_geno.lab_data_partitioned_normalized
    where tested_at is not null
--     where tested_at >= cast('2019-01-01' as date)
--       and state = 'California'
    group by 1, 2
    order by 1
)
select day_of_week,
       day_of_week_str,
       avg(batch_tested) as avg_batches_tested
from batches_tested_per_day
group by 1, 2
order by 1
;


--Make sure batches can only have one product_id
select batch_id,
       count(distinct product_id)
from dev_geno.lab_data_partitioned_normalized
group by 1
order by 2 desc;

--Validate that products always have multiple batches
select product_id,
       count(distinct batch_id)
from dev_geno.lab_data_partitioned_normalized
group by 1
order by 2 desc;