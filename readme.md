# Astra WRU
NodeJS app to generate writes to tables based on schema. With that, is possible to check the request write sizes for each table.

# Motivation

The right write size of the Cassandra workloads is complex to discover. Depending on the case, this lack of this information can cause the estimates to be underestimated.

In cases where it is needed to migrate a big cluster to Astra, doing this analysis consumes a lot of time, and it is hard to be sure of the numbers.

So, in conjunction with the CQL Proxy version that stores the metrics, this app will generate a simulated workload based on schema files to understand the behavior for each table.

# How it works

First thing, is to run CQL Proxy locally. It needed to use the version from [https://github.com/qzg/cql-proxy], that collects the read and write metrics.

Configure the app to connect to the local CQL Proxy.

The app reads all the schema files locates in the "schema" folder.

Then, for each table, the app will create the types and tables.

For each tables, the app generates 100 records (this quantity can be changed, if needed).

After all tasks are done, a file with the write sizes is generated in the out folder.



# Known issues

- Counter Tables fails (These tables do not accept inserts, so update commands needs to be implemented)
- Columns with UDT are not generated