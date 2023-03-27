import { randomBytes } from "crypto";
import { v4 as uuidv4, v1 as uuidv1 } from "uuid";
//Size in bits
//https://cassandra.apache.org/doc/latest/cassandra/cql/types.html
// String length
export const AVERAGE_STRING_LENGTH = 25;
export const AVERAGE_SET_LENGTH = 5;
export const AVERAGE_LIST_LENGTH = 5;
export const AVERAGE_BLOB_LENGTH = 500;


// Sizes in Bytes
export const dataTypeSizes = {
  ASCII: 8 * AVERAGE_STRING_LENGTH,
  BIGINT: 64,
  BLOB: 8 * AVERAGE_BLOB_LENGTH,
  BOOLEAN: 1,
  COUNTER: 64,
  DATE: 32,
  DECIMAL: 32,
  DOUBLE: 64,
  DURATION: 32,
  FLOAT: 32,
  INET: 8 * 16,
  INT: 32,
  SMALLINT: 16,
  TEXT: 8 * AVERAGE_STRING_LENGTH,
  TIME: 64,
  TIMESTAMP: 64,
  TIMEUUID: 128,
  TINYINT: 8,
  UUID: 128,
  VARCHAR: 8 * AVERAGE_STRING_LENGTH,
  VARINT: 32,
};

export const dataTypeGenerator = {
  ASCII: (col) =>
    `'${randomBytes(
      col.startsWith("COD_") ? 10 : AVERAGE_STRING_LENGTH / 2
    ).toString("hex")}'`,
  BIGINT: (col) => parseInt(Math.random() * 1000000000000000),
  BLOB: (col) => `textAsBlob('${randomBytes(AVERAGE_BLOB_LENGTH).toString("hex")}')`,
  BOOLEAN: (col) => Math.random() > 0.5,
  COUNTER: (col) => parseInt(Math.random() * 1000000000000000),
  DATE: (col) => `'${new Date().toISOString().substring(0, 10)}'`,
  DECIMAL: (col) => Math.random() * 10000000,
  DOUBLE: (col) => Math.random() * 10000000,
  DURATION: (col) => Math.random() * 10000000,
  FLOAT: (col) => Math.random() * 10000000,
  INET: (col) => `255.255.255.255`,
  INT: (col) => parseInt(Math.random() * 1000),
  SMALLINT: (col) => parseInt(Math.random() * 8),
  TEXT: (col) =>
    `'${randomBytes(
      col.startsWith("COD_") ? 10 : AVERAGE_STRING_LENGTH / 2
    ).toString("hex")}'`,
  TIME: (col) => `'${new Date().toISOString().substring(11, 23)}'`,
  TIMESTAMP: (col) => `'${new Date().toISOString()}'`,
  TIMEUUID: (col) => `${uuidv1()}`,
  TINYINT: (col) => parseInt(Math.random() * 8),
  UUID: (col) => `${uuidv4()}`,
  VARCHAR: (col) =>
    `'${randomBytes(
      col.startsWith("COD_") ? 10 : AVERAGE_STRING_LENGTH / 2
    ).toString("hex")}'`,
  VARINT: (col) => parseInt(Math.random() * 1000),
};
export const colTypeCollection = ["LIST", "SET", "MAP", "TUPLE"];

export const colTypeSpecial = [
  "LIST",
  "SET",
  "MAP",
  "TUPLE",
  "FROZEN",
  "STATIC",
];

export const colType = [
  "ASCII",
  "BIGINT",
  "BLOB",
  "BOOLEAN",
  "COUNTER",
  "DATE",
  "DECIMAL",
  "DOUBLE",
  "DURATION",
  "FLOAT",
  "INET",
  "INT",
  "SMALLINT",
  "TEXT",
  "TIME",
  "TIMESTAMP",
  "TIMEUUID",
  "TINYINT",
  "UUID",
  "VARCHAR",
  "VARINT",
  "LIST",
  "SET",
  "MAP",
  "TUPLE",
  "FROZEN",
];

export const keyspacesToIgnore = [
  "OpsCenter",
  "solr_admin",
  "dse_insights_local",
  "dse_system",
  "system_auth",
  "system_traces",
  "system",
  "dse_system_local",
  "system_distributed",
  "system_schema",
  "dse_perf",
  "dse_insights",
  "dse_security",
  "dse_leases",
  "HiveMetaStore",
  "dsefs",
  "DSE_ANALYTICS",
  "scylla_manager"
];

export const AstraUsageTables = ["astra_usage_stats", "astra_usage_histograms"];
