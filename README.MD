# Weedmaps Interview Code Challenge

## <a id="toc-table-of-contents"></a> Table of Contents:
------
- [Introduction](#toc-introduction)
- [Plan](#toc-plan)
- [Environment and files](#toc-environment)
- [Execution (What Happened)](#toc-execution)
- [Questions and Answers](#toc-questions)
- [Testing](#toc-testing)
- [Thoughts/Notes](#toc-notes)


## <a id="toc-introduction"></a>Introduction: 

This document serves to catalog my process, results, and all other discussion surrounding the weedmaps code test questions run against the lab data found on 2019-03-26T22:49:29-file.txt.gz 

Problem description/direction was as follows:

 1. Using the tool of your choice, acquire and stage the file for analysis.
 2. Derive the correct schema for the file.
 3. Expose the data in the file to some mechanism for executing queries.
 4. Using queries, answer the questions we have provided.
 5. Prepare a document containing your results and discussion of your methods. THIS DOCUMENT

## <a id="toc-plan"></a>Plan: 

After viewing the problem statement and pulling down the file the basic plan became immediatley clear.

1. Get data file into s3
2. View basic structure of json objects so I can define basic athena table schema, identify potential partitions
3. Get data into partitioned table in Athena in a proper columnar data format (I typically choose parquet) so that queries on the entire data set are viable and quick
4. Explore data and identify cleanliness issues
5. Create cleansed, partitioned, table with quick query response times for answering questions
6. Re-Test assumptions that final table dataset is good, and that answers to questsions make sense based on any data caveats
7. Document



[^back](#toc-table-of-contents)

## <a id="toc-environment"></a>Environment and files: 

The basic tooling I used for this was simply:

AWS S3
AWS Athena
Bash Shell
Jetbrains DataGrip

[^back](#toc-table-of-contents)

## <a id="toc-execution"></a>Plan Execution (What Happened): 

Immediatley it seemed this could be done on the cheap without any sort of EMR cluster running spark python jobs, and so instead of spinning up any sort of EC2 instance as a middle man for the data I used the aws cli to pull down the file locally. At this point, I just needed a basic view of the file so I could define an initial schema for an Athena table I could optimized after the fact for query speed. I simply ran zmore to get this information: 

![image](https://user-images.githubusercontent.com/22456230/136464814-765daf23-f276-4559-a6db-99eae94c0573.png)

Then used the aws cli to aws s3 cp to my test environment. The schema and basic initial table were then defined by the following [query](create_table.sql): 

```SQL
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
```

I decided not to do any setting of proper date types so I could explore the data in as raw a form as possible so I could locate anomalies and hopefully nothing would break on the initial table create.

State seemed like a good column to partition on, so after the following long running ```select distinct state``` that required extending the DML query timeout to 60 minutes, I had an idea of what to partition on, and had identified at least one of the data consistency issues. 

There were two issues. One, a simple ```create table as``` partitioning on state would not work because there were 144 different variations of state identifier (greater than 100 a cta would allow), and two, state identifiers were not consistent.

Normalizing states would be easy once I had a quick table to query against so I pulled the results of the ```select distinct state``` [query](find_states.sql) SEEN [here](state_values_raw.csv) 

The quickest way to create a fast table to query was just to use the CTAs strategy and artificially limit the create to 100 partitions, which was done by identifying the 100th entry from the results file above, and then insert the rest. Additionally, I would create the datafiles using PARQUET format to get the data into a proper columnar datastore. The queries used to accomplish this were:

```sql
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
```

 Note* in a real life situation where this sort of data is streaming and we could get any value at any time I would have handled this differently, perhaps with lambdas, but it was a small number of values and easy to deal with manually using ```where state <= 'OR` ```

 At this point, most queries run in a few seconds, and the worst I could do was a select distinct on batch_id which would take 5 minutes, so I began the process of examining the data for issues.

 ### Data Issues, Cleansing and Final data Table

 The first issue to handle was the varying style of state codes. It did look like all the values were valid, just not in the same style of format. So a quick google on state abbreviations brought me to [stateabbreviations.us](https://www.stateabbreviations.us/) where I found a table with all the values I'd seen from the data that allowed me to create [this csv](StateCodes.csv) to build a mapping table to join to. Query for mapping table is [here](create_statecodes_tbl.sql).

 Next I ran a [series of crude selects](data_debug.sql) against each column to attempt to identify any obvious problems with the data that I could handle or would need to query around when answering the questions. The obvious issues were the following:

 1. batch_id, vendor_id, product_id, and lab_id had null values for some rows which I took to mean was an invalid row
 2. tested_at and expires_at had varying format for data. Primarily the date format using / as a separator would cause problems directly casting those values to a date
 3. thc, thca, cbd, and cbda had negative values for some rows. I have assumed those are invalid rows, but I did ask for clarification via email on whether or not this was a sort of lab reporting I did not inuitively understand and could take the absolute value of or apply some other function to make the data viable.

 I opted to leave the invalid rows in the final table for measuring lab reporting accuracy, and created a final partitioned, normalized data table as follows:

```sql
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
```

At this point I had a very fast and clean table I could answer the questions with.

[^back](#toc-table-of-contents)

## <a id="toc-questions"></a>Questions, Answers and Discussion: 

Queries for each of the questions and their run times are available in [this file](questions.sql)

Based on the constraint that no labs, vendors, or products churn, I took this to mean that I should look over all time and not products tested in the most recent week or some other timeframe as everything should still be an active entity worth counting.

### **1. Which 5 vendors have the most products?**
I've made the assumption here that counting distinct product_id's is the right way to do this, as only a product_id constitutes a product and that the various batches of a product_id are all still the same product_id and thus count as one.

I was careful to exclude the data anomalies I'd found previously in the where clause.

There appeared to be many vendors with the same number of products, so I assumed a dense_rank of the vendors with the top 5 highest amounts of products was the proper way to answer this. This output has 1794 distinct vendors with the top 5 highest amounts of products. 

This gave me pause at first and led to me checking for various data anomalies I may have missed....

### 2. Which 5 vendors have the fewest products?
Similar to 1, but I simply inverted the order by's in the query to asc sort rather than desc.

Again it seemed like vendors all had similar amounts of distinct product_id's with there ultimately being 1890 vendors with the bottom 5 amount of products using a dense_rank() function.

### 3. Which 5 products have the highest potency?
Since products are tested weekly and have multiple batches, the most correct way to look at this seemed to be to look at the total potency for the most recent batch. I couldn't find any instances of there being multiple batches for a product on the same test date so I didn't see the need to average or take a max. I considered the notion of averaging potencies together, but that didn't seem right as consumers are not purchasing a blend of batches for a product as far as I know.

### 4. Which 5 products have the lowest potency?


### 5. Which 5 labs have the highest accuracy?
### 6. Which 5 labs have the lowest accuracy?


### 7. Which 5 states have the most products and how many?
[Query Result](question_7_and_8_query_output.csv)
```
state,num_products,rank
California,1306222,1
Oregon,215317,2
Colorado,207020,3
Washington,153847,4
North Dakota,11686,5
```

### 8. Which 5 states have the fewest products and how few?
```
state,num_products,rank
Montana,5325,1
New Hampshire,5511,2
Wyoming,5551,3
Arkansas,5552,4
North Carolina,5833,5
```

### 9. How many tests are performed each day of the week?

[^back](#toc-table-of-contents)

## <a id="toc-testing"></a>Testing:

lab_data rows: 233,225,647
lab_data_partitioned rows: 233,225,647
lab_data_partitioned_normalized rows: 233,225,647

[^back](#toc-table-of-contents)

## <a id="toc-notes"></a>Thoughts and Notes after the fact: 

After the fact thoughts and notes and things I would have done if time and budget weren't as constrained as they were.

[^back](#toc-table-of-contents)
