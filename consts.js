import { randomBytes } from "crypto";
import lodash from 'lodash';
const { random, isString, uniq } = lodash;
import { v4 as uuidv4, v1 as uuidv1 } from "uuid";

// Min and Max length to consider while generating records
// For the schema calculation, the max length is considered
export const AVERAGE_STRING_LENGTH = [5, 40];
export const AVERAGE_SET_LENGTH = [1, 5];
export const AVERAGE_LIST_LENGTH = [1, 5];
export const AVERAGE_BLOB_LENGTH = [200, 1000];


//Size in bytes
//https://cassandra.apache.org/doc/latest/cassandra/cql/types.html
export const dataTypeSizes = {
  ASCII: AVERAGE_STRING_LENGTH[1],
  BIGINT: 8,
  BLOB: AVERAGE_BLOB_LENGTH[1],
  BOOLEAN: 1,
  COUNTER: 8,
  DATE: 8,
  DECIMAL: 8,
  DOUBLE: 8,
  DURATION: 4,
  FLOAT: 4,
  INET: 16,
  INT: 4,
  SMALLINT: 2,
  TEXT: AVERAGE_STRING_LENGTH[1],
  TIME: 8,
  TIMESTAMP: 8,
  TIMEUUID: 16,
  TINYINT: 1,
  UUID: 16,
  VARCHAR: AVERAGE_STRING_LENGTH[1],
  VARINT: 8,
};


export const dataTypeGenerator = {
  ASCII: (col) =>
    `'${randomBytes(
      col.startsWith("COD_") ? 10 : random(AVERAGE_STRING_LENGTH[0], AVERAGE_STRING_LENGTH[1]) / 2
    ).toString("hex")}'`,
  BIGINT: (col) => parseInt(Math.random() * 1000000000000000),
  BLOB: (col) => `textAsBlob('${randomBytes(random(AVERAGE_BLOB_LENGTH[0], AVERAGE_BLOB_LENGTH[1])).toString("hex")}')`,
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
      col.startsWith("COD_") ? 10 : random(AVERAGE_STRING_LENGTH[0], AVERAGE_STRING_LENGTH[1]) / 2
    ).toString("hex")}'`,
  TIME: (col) => `'${new Date().toISOString().substring(11, 23)}'`,
  TIMESTAMP: (col) => `'${new Date().toISOString()}'`,
  TIMEUUID: (col) => `${uuidv1()}`,
  TINYINT: (col) => parseInt(Math.random() * 8),
  UUID: (col) => `${uuidv4()}`,
  VARCHAR: (col) =>
    `'${randomBytes(
      col.startsWith("COD_") ? 10 : random(AVERAGE_STRING_LENGTH[0], AVERAGE_STRING_LENGTH[1]) / 2
    ).toString("hex")}'`,
  VARINT: (col) => parseInt(Math.random() * 1000),
};

export async function generateValue(name, type, udts) {
  const first = type.indexOf('<')
  if (first > 0) {
    const advType = type.substr(0, first)
    const innerType = type.substring(first + 1, type.length - 1)

    if (advType === 'MAP') {
      const elems = innerType.split(",").map(e => e.trim())
      const len = random(AVERAGE_SET_LENGTH[0], AVERAGE_SET_LENGTH[1])
      let res = []
      for (let i = 0; i < len; i++) {
        let k = await generateValue(name, elems[0], udts)
        let v = await generateValue(name, elems[1], udts)
        // if (isString(k)) k = k.replace(/'/g, "")
        // if (isString(v)) v = v.replace(/'/g, "")
        res.push(`${k} : ${v} `)
      }

      // return JSON.stringify(res).replace(/"/gm, "'")
      return `{ ${res.join(" , ")}} `
    }
    else if (advType === 'SET' || advType === 'LIST') {
      const len = random(AVERAGE_SET_LENGTH[0], AVERAGE_SET_LENGTH[1])
      const res = []
      for (let i = 0; i < len; i++) {
        let v = await generateValue(name, innerType, udts)
        res.push(v)
      }
      return advType === 'SET' ? `{ ${uniq(res).join(',')} }` : `[ ${res.join(',')} ]`
    }
    else if (advType === 'TUPLE') {
      const res = await await Promise.all(innerType.split(',').map(async e => await generateValue(name, e.trim(), udts)))
      return `( ${res.join(',')} )`
    }
    else return await generateValue(name, innerType, udts)
  } else if (dataTypeGenerator[type]) {
    return dataTypeGenerator[type](name)
  } else {
    // console.log("DT:", name, type)
    return await generateValueUDT(name, type, udts)
  }

  throw new Error(`Invalid type: ${type}`)
}

async function generateValueUDT(name, type, udts) {
  const udt = udts.find(e => type === `${e.keyspace}.${e.name}`.toUpperCase())
  if (udt) {
    const res = []
    for (const col of udt.columns) {
      const v = await generateValue(col.name, col.definition, udts)
      res.push(`${col.name} : ${v} `)
    }
    return `{ ${res.join(" , ")}} `
  }

  throw new Error(`User defined type not found: ${type}`)
}

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
