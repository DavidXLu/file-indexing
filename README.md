# file-indexing
Tool class to index and store static records to file system.
* IndexWriter generates index file from source file, multi keys supported.
* IndexReader loads records by keys with source file and generated file.
* Load test is provided.
* It takes 5-30 minutes to generates index file for a 16GB file(20M records)
* Load test with 16GB file(20M records), 100 concurrency, 50 records per load over 5 days, average time cost less than 10 ms for each load. Memory and CPU usage is stable.
!!! Caution: GIT will change line break based on OS. Please avoid check source file in to GIT. It will break indexing.