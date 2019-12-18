# Dataflow - BigQuery - Schema Evolution Solutions

This repository provides ideas on how to handle schema evolution in a streaming pipeline using GCP PubSub -> Dataflow 
-> BigQuery. The goal here is how to handle schema changes (e.g. new versions of the streaming messages) in 
the ingestion pipeline (i.e. Dataflow) and the destination (i.e BigQuery)

The work provided here is based on the following repos
* [dataflow-dynamic-schema](https://github.com/ryanmcdowell/dataflow-dynamic-schema)
* [dataflow-bigquery-dynamic-destinations](https://github.com/ryanmcdowell/dataflow-bigquery-dynamic-destinations) 

## Getting Started

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.
```sh
mvn clean && mvn compile
```


## Solution 1: Using BigQuery Dynamic Destinations and Union-Views

### Concept

A first and quick solution for schema evolution is to capture all versions of the incoming messages using the same
ingestion dataflow job and store them in different BigQuery tables, one for each version (e.g. table employee_v1, employee_v2, 
etc). 
On top of these base tables a view should be manually created and maintained to provide an abstraction over the
topic (e.g. employee) that ```union all```  the underlying tables and contains the schema resolution logic (e.g. adding, 
dropping, renaming or changing data type of fields)

In order to achieve this the pipeline should be able to:
* Automatically identify the schema of the messages (i.e. by inferring it or lookup from a schema registry)
    * In this example we use Avro GenericRecord for the payload and infer the BigQuery Schema from it. 
    * Refer to [pubsub-publish-avro-genericrecord](https://github.com/kwadie/pubsub-publish-avro-genericrecord) 
    for the producer/publisher code and to publish sample events for the demo
* Automatically converts the parsed schema to a BigQuery schema
    * [PubsubAvroToTableRowFn](src/main/java/com/google/cloud/pso/dofn/PubsubAvroToTableRowFn.java) is responsable
            for that. Generalize from it if you want to use another serialization format
* Create the table if it doesn't exist in BigQuery
    * By using ```CreateDisposition.CREATE_IF_NEEDED``` at the BigQuery sink and constructing the table schema at
      runtime
* Route every message to it's corresponding destination table
    * By using BigQueryIO [DynamicDestinations](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/io/gcp/bigquery/DynamicDestinations.html) 
    * By constructing the destination table name from the message attributes. In this code sample it uses the "doc"
      attribute from the Avro payload

__Pros__
* Easy and fast to implement (i.e. no schema registry needed)
* Automatically captures and stores all incoming messages regardless of schema evolution constraints 
(e.g. backward/forward compatibility)

__Cons__
* Doesn't provide organization-wide contracts about schema changes between producers and consumers
* The abstraction view is maintained manually

__Extensions__
* Other serialization formats than Avro (e.g. JSON, XML, Proto) could be used given that a replacement for 
  [PubsubAvroToTableRowFn](src/main/java/com/google/cloud/pso/dofn/PubsubAvroToTableRowFn.java) is provided to
  transform PubSub elements to BigQuery TableRow elements
         * For example, use JSON messages with the schema version attached as an attribute that is then fetched from 
           a schema registry and converted to a BigQuery schema 
* Handle arrays and logical data types in conversion from Avro to BigQuery Schema in [BigQueryAvroUtils](src/main/java/com/google/cloud/pso/bigquery/BigQueryAvroUtils.java)
* Instead of passing the schema with every element, pass the version only and look up the schema from a schema registry
  to parse the pubsub message
* Handle the error output (Tag) from [PubsubAvroToTableRowFn](src/main/java/com/google/cloud/pso/dofn/PubsubAvroToTableRowFn.java)  
* Use another field other than "doc" to pass the schema version

__Limitations__
* If the producer sends different schema with the same version number, one of the schema will get created as a BigQuery 
  Table and the messages that maps to this schema will get inserted, however, the other messages with the different
  schema will get rejected by BQ and redirected to the error output (Tag) (which is not handled yet in this code sample)

### Demo Usage
#### Run ingestion pipeline
```mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pso.pipeline.PubsubAvroToBigQueryDynamicDestinations \
   -Dexec.args=" \
   --subscription=projects/<project>/subscriptions/<subscription> \
   --outputTableProject=<GCP project for BigQuery outout tables> \
   --outputTableDataset=<BigQuery dataset for outout tables> \
   --project=<GCP project to run Dataflow> \
   --streaming=true \
   --runner=DataflowRunner"

```

#### Publish sample Avro events
1. Clone this repository [pubsub-publish-avro-genericrecord](https://github.com/kwadie/pubsub-publish-avro-genericrecord)
2. Build the project using the repo [instructions](https://github.com/kwadie/pubsub-publish-avro-genericrecord#building-the-project)
3. Open the [employee.avsc](https://github.com/kwadie/pubsub-publish-avro-genericrecord/blob/master/src/main/avro/employee.avsc)
  file and edit the "doc" field to contain the BigQuery table name without project and dataset info (e.g. employee_1)
4. Execute the application to publish sample messages using the repo [instructions](https://github.com/kwadie/pubsub-publish-avro-genericrecord#building-the-project)  
5. Repeat the process from step 3 to publish at least another set of messages but 
with a different version (e.g. employee_2). Note that for simplicity, the schema itself (i.e. fields)
is the same to avoid changing the code but what we demo here is the ability to 
dynamically create tables and route messages to them  
6. Check your BigQuery Dataset to make sure that new "employee_x" tables are created and populated

## Solution 2: Using BigQuery Dynamic Schema
TODO

## Solution 3: Using Schema Registry
TODO






