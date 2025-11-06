# Proposed Solution

## Summary
Key concept is to use the "Medallion Architecture":
1. Land raw data into MinIO (BRONZE)
2. Produce large, partially flattened tables into Apache Iceberg. (SILVER)
3. Produce analytics-ready datasets from the Apache Iceberg table. (GOLD)

Producing a large flat table for the SILVER tier is an intentional decision due to the nature of Apache Iceberg. In a traditional RDBMS, it is best practice to normalise data to reduce data redundancy and allow for easier schema extension. Because Apache Iceberg supports schema evolution, and is a compressed Parquet format this requirement is no longer needed. Keeping data in a single wide table means that joins do not need to be conducted which are expensive operations.

The GOLD tier contains example datasets to answer specific business questions by querying the SILVER tier. Because all of the data is in one location, we can register pre-configured "virtual" views of the data, without having to copy the data again. The query engine is able to work from the stored SQL relating to the view and query only the necessary data in the underlying Parquet files managed by Iceberg.

## Technology Choices
This section discusses the technology choices made as part of this assessment.  

### Storage Tier - MinIO
MinIO is an S3 compatible object store, which is widely used for Data Lake storage. Due to its S3 compatibility, this allows the rest of the solution to be AWS-compatible with minimal code changes required.  

It also adds a level of realism to the exercise, processing data directly from the source directory of the processing application is unrealistic; using MinIO allows the data to be fetched from the Data Lake.  

The use of an S3-compatible storage tier also means that we can use Apache Spark's S3 support (after building the necessary JARs into the Spark docker image). Additionally, Apache Iceberg supports S3 as a storage technology for its table format.

### Transformation - Apache Spark
Apache Spark is a widely used data processing toolchain, built on top of the Hadoop ecosystem. This allows MapReduce style operations to be expressed as simple DataFrame transformations, which are easy to use as a developer. Equally Apache Spark provides a SQL API which can be used to execute raw ANSI SQL against the data, if desired.  

Apache Spark is well-supported in the big data ecosystem, and additionally well supported in AWS. Services such as Amazon EMR and AWS Glue both provide a runtime for Apache Spark.  

As such the technology choice allows for a scalable approach (increase nodes in the cluster - scale out) as well as flexibility of deployment options (local, on-prem, cloud).

### Table Format - Apache Iceberg
Apache Iceberg is an open table format build on top of Apache Parquet. It is essentially Apache Parquet with additional metadata, and a wide variety of additional features. Some of the additional features provide great value for this type of problem. Schema Evolution makes it easy to enrich the table and store additional columns, whereas incremental upserts, snapshotting and time travel make it easy to add/update data and view data as it was at some previous point in time.

Because Apache Parquet is built over Apache Parquet, it enables features which allow for efficient querying such as predicate and projection pushdown. Predicate pushdown works when data is appropriately partitioned by using the metadata to ensure that only the necessary parts of the data are read, this is supported natively by services such as Amazon Athena, where this can introduce significant cost savings (as it is charged by data retrieved). Projection pushdown is supported as columns can be selected directly from the Parquet file due to its columnar nature.

Nessie is also deployed as an Apache Iceberg metadata store.

## Running the solution
__note: the commands here assume it is being run on Linux with bash__
### Deploying the Stack
Configure the application by copying the example .env file, and modifying as appropriate.
```bash
cp .env.example .env`
source .env
```

Run up the docker-compose stack.
`docker compose up [-d] [--build]`
*Note*: the build for the Apache Spark docker image is a lengthy process. This is because the commonly used `bitnami/spark` has recently been moved behind a paywall, and so an image has been built from scratch for this. In reality, we would push this image to an image registry to save it being continuously rebuilt.

### Running the Spark Jobs
We use the Spark master node to submit the job, as in the compose file we have mapped the `spark/jobs` directory into this container. This is easier than creating another container to submit the jobs from.  
`docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/etl.py`

### Running Unit Tests
Because PySpark requires a JVM to run, a Dockerfile is provided in case your environment does not have Java installed. It is recommended to run the unit tests as below:  

```bash
cd spark
docker build -t exa-tests -f ./test/Dockerfile .
docker run --rm exa-tests
```

# Future Enhancements
1. Partition the SILVER table to make GOLD queries more efficient.
2. Update to Spark Structured Streaming so that the Apache Iceberg will continuously update.
3. Demonstrate Apache Iceberg time-travel, compaction etc...
4. Integration tests.
