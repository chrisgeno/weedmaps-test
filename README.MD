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

I decided right away that this coud be done on the cheap without any sort of EMR cluster running spark python jobs, and so instead of spinning up any sort of EC2 instance as a middle man for the data I used the aws cli to pull down the file locally. At this point, I just needed a basic view of the file so I could define an initial schema for an Athena table I could optimized after the fact for query speed. I simply ran zmore to get this information: 

[!zmore](./zmore.svg)



[^back](#toc-table-of-contents)

## <a id="toc-questions"></a>Questions and Answers and Discussion: 

### 1. Which 5 vendors have the most products?
### 2. Which 5 vendors have the fewest products?
### 3. Which 5 products have the highest potency?
### 4. Which 5 products have the lowest potency?
### 5. Which 5 labs have the highest accuracy?
### 6. Which 5 labs have the lowest accuracy?
### 7. Which 5 states have the most products and how many?
### 8. Which 5 states have the fewest products and how few?
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