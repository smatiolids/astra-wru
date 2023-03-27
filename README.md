# Astra WRU
NodeJS app to generate writes to tables based on the schema. With this process, the request write sizes for each table can be checked.

# Motivation

The right write size of the Cassandra workloads is complex to discover. Depending on the case, this lack of this information can cause the estimates to be underestimated.

In cases where it is needed to migrate a big cluster to Astra, doing this analysis consumes a lot of time, and it is hard to be sure of the numbers.

So, in conjunction with the CQL Proxy version that stores the metrics, this app will generate a simulated workload based on schema files to understand the behavior for each table.

# How it works

The first thing is to run CQL Proxy locally. It needed to use the version from [https://github.com/qzg/cql-proxy], which collects the read and write metrics.

Configure the app to connect to the local CQL Proxy.

The app reads all the schema files from the "schema" folder.

Then, the app will create the types and tables for each table.

For each table, the app generates 100 records (this quantity can be changed if needed).

After finishing all tasks, a file with the write sizes is generated in the "out" folder.

# How to use it

## Pre requisities

- An Astra Database and Keyspace specifically for this process.
- Golang (to build the CQL Proxy)
- NodeJS

## Setting environment variables

Rename and .env_sample file to .env.

Update the variables with the correct information.

## Starting the CQL Proxy

Clone the git repo from https://github.com/qzg/cql-proxy.

Build it following the instructions available in the readme.

Start it with the following parameters:
- --astra-bundle
- --username
- --password
- --track-usage 
- --usage-keyspace app

The file start-cql-proxy.sh can be used to start the CQL Proxy

## Installing dependencies

In the app root folder, run

````
npm install

````

## Running the app

- Place the schemas files in the "schemas" folder
- Run

````
node index 
```


## Post running

- Table info available in "out" folder, in the file "table_stats.csv"
- Logs and errors also available in the "out" folder



# Known issues

- Counter Tables fails (These tables do not accept inserts, so update commands need to be implemented)
- Columns with UDT are not generated
