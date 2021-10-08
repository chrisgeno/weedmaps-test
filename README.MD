# Weedmaps Interview Code Challenge

This document and all supporting files are available at https://github.com/chrisgeno/weedmaps-test It may be easier to view the readme with it's markdown and links to files referenced within it from there rather than locally.

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

 Note that throughout the various sql files, and code snippets, some comments have been left in, and in the [questions.sql](questions.sql) file some exploratry queries related to understanding the data used to answer the question have been left in. Normally I would clean this sort of thing up, but in this case I thought it useful as a way to give insight into my thought process and ongoing testing that I like to do to verify as I go.

## <a id="toc-plan"></a>Plan: 

After viewing the problem statement and pulling down the file the basic plan became immediatley clear.

1. Get data file into s3
2. View basic structure of json objects so I can define basic athena table schema, identify potential partitions
3. Take every opportunity from this point on to observe data for quality issues.
4. Get data into partitioned table in Athena in a proper columnar data format (I typically choose parquet) so that queries on the entire data set are viable and quick
5. Explore data and identify cleanliness issues
6. Create cleansed, partitioned, table with quick query response times for answering questions
7. Re-Test assumptions that final table dataset is good, and that answers to questsions make sense based on any data caveats
8. Document



[^back](#toc-table-of-contents)

## <a id="toc-environment"></a>Environment and files: 

The basic tooling I used for this was simply:

* AWS S3
* AWS Athena
* Bash Shell
* Jetbrains DataGrip

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

I decided not to do any setting of proper date types other than the obvious floats so I could explore the data in as raw a form as possible. This would let me locate anomalies and hopefully nothing would break on the initial table create.

State seemed like a good column to partition on, so after a long running ```select distinct state``` [query](find_states.sql) that required extending the query timeout to 60 minutes, I had an idea of what to partition on, and had identified at least one of the data consistency issues. 

There were two issues. One, a simple ```create table as``` partitioning on state would not work because there were 144 different variations of state identifier (greater than 100 a cta would allow), and two, state identifiers were not consistent.

Normalizing states would be easy once I had a quick table to query against so I pulled the results of the ```select distinct state``` [query](find_states.sql) SEEN [here](state_values_raw.csv) 

The quickest way to create a fast table to query was just to use the CTAs strategy and artificially limit the create to 100 partitions, which was done by identifying the 100th entry from the results file above, and then insert the rest. Additionally, I would create the datafiles using PARQUET format to get the data into a proper columnar datastore. The queries used to accomplish this were:

```sql
CREATE TABLE lab_data_partitioned
    WITH (
        format = 'PARQUET',
        external_location = 's3://salsify-geno-dev/queryresults/',
        partitioned_by = ARRAY ['state']
        --bucketed_by = ARRAY['product_id'],
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

 The first issue to handle was the varying style of state codes. It did look like all the values were valid, just not in the same format. So a quick google on state abbreviations brought me to [stateabbreviations.us](https://www.stateabbreviations.us/) where I found a table with all the values I'd seen from the data that allowed me to create [this csv](StateCodes.csv) to build a mapping table to join to. Query for mapping table is [here](create_statecodes_tbl.sql).

 Next I ran a [series of crude selects](data_debug.sql) against each column to attempt to identify any obvious problems with the data that I could handle or would need to query around when answering the questions. The obvious issues were the following:

 1. batch_id, vendor_id, product_id, and lab_id had null values for some rows which I took to mean was an invalid row
 2. tested_at and expires_at had varying formats for data. Primarily the date format using / as a separator would cause problems directly casting those values to a date. Nulls also in these columns.
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

Based on the constraint that no labs, vendors, or products churn, I took this to mean that I should look over all time and not products tested in the most recent week or some other timeframe as everything should still be an active entity worth counting except where noted in the highest/lowest potency questions.

Below I'll provide some discussion on each problem and a link to results.

### **1. Which 5 vendors have the most products?**
[Output here](question_1_query_output.csv)

I've made the assumption here that counting distinct product_id's is the right way to do this, as only a product_id constitutes a product and that the various batches of a product_id are all still the same product_id and thus count as one. If this was an incorrect assumption simpy removing the distincts from this query would yield a result with a much smaller list effectively counting each batch as a product.

I was careful to exclude the data anomalies I'd found previously in the where clause.

There appeared to be many vendors with the same number of products, so I assumed a dense_rank of the vendors with the top 5 highest amounts of products was the proper way to answer this. This output has 1794 distinct vendors with the top 5 highest amounts of products. 

This gave me pause at first and led to me checking for various data anomalies I may have missed....

### 2. Which 5 vendors have the fewest products?
[Output here](questio_2_query_output.csv)

Similar to 1, but I simply inverted the order by's in the query to asc sort rather than desc.

Again it seemed like many vendors all had similar amounts of distinct product_id's with there ultimately being 1890 vendors with the bottom 5 amount of products using a dense_rank() function.

### 3. Which 5 products have the highest potency?
[Output here](question_3_query_output.csv)

Since products are tested weekly and have multiple batches, the most correct way to look at this seemed to be to look at the total potency for the most recent batch. I couldn't find any instances of there being multiple batches for a product on the same test date so I didn't see the need to average or take a max. I considered the notion of averaging potencies together, but that didn't seem right as consumers are not purchasing a blend of batches for a product as far as I know. Basically, I've assumed that the batch that is available for a product, is the batch that was tested most recently.

### 4. Which 5 products have the lowest potency?
[Output Here](question_4_query_output.csv)

Same assumptions as in 3, but with the desc changed to asc.


### 5. Which 5 labs have the highest accuracy?

#### ITERATION 1:

For this question I'm going to define accuracy as not only being a valid, useable data point for each column, but as also being the proper format for reporting. The rules I'll be using for valid vs accurate are as follows:

1. batch_id, vendor_id, product_id, lab_id: are not null
2. tested_at, expires_at: not null and must be in a US date format separated by '-' an easily castable date value. In this case using '/' is valid and can be handled, but is not accurate.
3. thc, thca, cbd, cbda: are not null and have values >= 0. It is unclear what a negative value represents and so this is considered invalid data. *Note I had considered running statistics on these values to find extreme outliers as inaccurate readings, but as I have no reference for what is reality in this situation I opted against it for time.
4. State is fully written out as the proper state name and not a code or abbreviation. IE California, and not CA or Calif.

[Query Result](question_5_query_output.csv)

#### ITERATION 2:

Having done this calculation based on the above data and having explored the state identifiers over the years. It seemed like that assumption may have been bad as standards may have evolved and those labs may very well have been providing accurate state data at the time. I decided I should attempt a second iteration where, since all state data was actually a valid identifier except for where null, that should be left out of the accuracy calculation. Effectively only measuring inaccuracy as labs with negative values in their potency readings. That query is also in the [questions.sql](questions.sql) file and those results are here: [Query 5 and 6 Iteration 2 output](question_5_and_6_ITERATION2_query_output.csv)




### 6. Which 5 labs have the lowest accuracy?

####ITERATION 1:

For the initial set of assumptions used in 5 the query results for lowest accuracy are here: [Query Result](question_6_query_output.csv)

#### ITERATION 2:

[Query 5 and 6 Iteration 2 output](question_5_and_6_ITERATION2_query_output.csv)

### 7. Which 5 states have the most products and how many?
[Query Result](question_7_and_8_query_output.csv)

After cleaning up and normalizing the state column, this was a fairly straightforward ```count(distinct product_id)``` being careful to exclude null product_id's since some rows seem to have state and potency data and nothing else. Note in this case, I did not filter out products that had negative potency values. Though the test was bad, it did seem like this was still a product that was available and thus worth counting.

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
[Query Result](question_9_query_output.csv)

It was unclear what the proper timeframe for the week was, so I assumed that we wanted the average number of tests taken per day over the entirety of the time period rather than a specific week. I reached out for clarification but ended up moving forward with this assumption due to time constraints.

First I verified that every batch_id corresponded to a test, and that there were no batch_id's with multiple tested_at dates. This meant that I could count batch_id's per day with a day of week function in a CTE. At this point it was a simple matter of averaging the number of tests per day.

```
day_of_week,day_of_week_str,avg_batches_tested
1,Mon,335794.34
2,Tue,448277.8686868687
3,Wed,560814.85
4,Thu,448304.6
5,Fri,223372.54
6,Sat,110838.18
7,Sun,110845.9504950495
```


[^back](#toc-table-of-contents)

## <a id="toc-testing"></a>Testing:

My testing process is usually continuous throughout with checks before and after each stage of development, with testing and sanity checks at the end. For example, when creating each of my tables I would check basic things like row counts to make sure I hadn't caused inadvertent duplication. Normally I would maintain statistics on certain columns and their values, but I kept it very basic for this exercise due to time constraints. 

- lab_data rows: 233,225,647
- lab_data_partitioned rows: 233,225,647
- lab_data_partitioned_normalized rows: 233,225,647

This actualy saved me when created the partitioned_normalized table as I had initially used a join that used an array contains function, which caused some duplication, likely on nulls, which led me to explicitly state the state mappings for normalization.

Much of the in process testing can be seen in the questions.sql file as I went. 

I was particularly concerned about questions 1 and 2 where it seemed that many vendors had the same amount of products. I tested this against counting batches rather than products, and that yielded a better looking list but it still did not seem the correct way to me. Eventually I even ran the queries agains the slow lab_data table just to verify that those results matched, and they did.

[^back](#toc-table-of-contents)

## <a id="toc-notes"></a>Final Thoughts and Notes after the fact: 

 The data seemed to have fairly common cleanliness issues, and I believe I found most if not all of them. However, I do feel like I handled them in a one off way consistent with this static data set. In production I would have likely handled this differently as the data streamed in, and set up some sort of heuristic for handling unknown variations in states, and id's. 

[^back](#toc-table-of-contents)
