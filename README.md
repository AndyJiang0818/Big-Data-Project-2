# MapReduce and Spark Implementation

## Overview
1. Given m documents, compute the term-term relevance using MapReduce algorithm and Spark implementation. 
2. Input a text file, each line represents a document. 
3. Poutput a list of term-term paris sorted by their similarity descending. 
4. Compute Term Frequency - inverse Document Frenquency (TF-IDF) for each term. 
5. Compute and sort term-term relevance between a query term and all terms associated with the TF-IDF matrix. 

## Run
### Step 1:
- In line 22, change the file name 'project2_test.txt' to the file name that you named. 
- OR just using the 'project2_test.txt' file that I attached in this zip file. 

### Step 2:
- Install numpy package by using the command 'pip install numpy'. 
- Install pyspark package by using the command 'pip install pyspark'.
- Install psutil package by using the command 'pip install psutil'.

### Step 3:
- Put the code into a python ide and run the code. 
- OR run the code in the command line interface. 

## Potential Improvements 
1. Try to reduce operations like gropByKey(), reducebyKey(), join().
- The function groupBykey must hold all the key-value pair in memory and if a key
has too many values, it can cause an out of memory error.

2. Reduce shuffling
- Spark uses shuffling to redistribute data.
- Shuffling is an expensive operation.

3. Caching
- Spark will store the dataset in memory which allows for faster access and
retrieval.

4. Dynamic allocation
- Scaling up or down based number of executors based on workload.

5. Data Skewing
- There might be uneven distribution of data which reduces utilization. 

6. Optimize the amount of Spark partitions
- Too much or too little spark partitions could mean some executors are idle or
scheduling overhead.

7. Use mapPartitions() over map()
- Using mapPartitions provides initialization for many RDD elements rather once
per RDD element.

8. Check for memory leaks
- Unchecked memory leaks can cause a host of memory issues and slow data
processing.

9. Check for bottlenecks
- Bottlenecks can occur in any stage of our algorithm which can often slow data
processing.

10. Improve queries
- Instead of returning every row or column we should only return the ones we are
looking for. 
