# Astra Write Size Estimator

The purpose of this code is to generate data based on schema files to understand the write sizes.

## How it works

This estimator runs together with cql-proxy (Kyu Gabriel version, with the metrics collector: https://github.com/qzg/cql-proxy.git) to generate the statistics on Astra.

With the CQL Proxy in place, it reads the schema files from the schema folder and, for each file, it creates types and tables to generate and insert records to the tables.

After that, the created objets are dropped.

The statistics can be found on Astra Usage Stats table: astra_usage_stats and astra_usage_histograms

# Known issues:

- Fields with advanced data types (Sets, maps, tuples...) or user defined types are considered on table creation, but the generated records DOES NOT include these fields
- Records for counter tables are not generated
- BLOB field generator has error