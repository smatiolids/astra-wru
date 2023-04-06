import dotenv from "dotenv";
import { Client, types } from "cassandra-driver";
import { processSchema } from "./schema.js";
import { AstraUsageTables, dataTypeGenerator } from "./consts.js";
import { readdir } from "fs/promises";
import { createLogger, format, transports } from "winston";
import { createWriteStream } from "fs";
import _ from 'lodash';
const { max, sortBy } = _;

dotenv.config();

const ks = process.env.ASTRA_KEYSPACE;

/**
 * Astra WRU Parameters
 */
const DROP_TABLES = true;
const CREATE_TABLES = true;
const WRITE_RECORDS = true;
const CLEAR_STATS = true;
const NUM_RECORDS = 100; // Records to insert
const RRU_SIZE = 4000;
const SELECT_LIMIT = 20; // Records to select

const logger = createLogger({
  level: "info",
  format: format.json(),
  defaultMeta: { service: "user-service" },
  format: format.combine(format.timestamp(), format.json()),
  transports: [
    new transports.Console(),
    new transports.File({
      filename: "./out/error.log",
      level: "error",
    }),
    new transports.File({ filename: "./out/combined.log" }),
  ],
});



async function processSchemaFile(client, file) {
  logger.info(`Starting reading ${file}`);

  // Extract tables from schema file
  const tables = await processSchema(file);

  if (CREATE_TABLES) {
    const errors = [];
    for (let i = 0; i < tables.length; i++) {


      const tab = tables[i];

      if (["TABLE", "TYPE"].includes(tab.objtype)) {
        const stmt_create = `CREATE ${tab.objtype}  IF NOT EXISTS  ${ks}.${tab.keyspace}_${tab.name} ${tab.colDefinition}`;

        try {
          const rs = await client.execute(stmt_create);
          logger.info(
            `Object created:  ${tab.objtype} ${ks}.${tab.keyspace}_${tab.name}`
          );
        } catch (error) {
          logger.error(
            `Error creating Object:  ${tab.objtype} ${ks}.${tab.keyspace}_${tab.name}`
          );
          logger.error(stmt_create);
          logger.error(error);
        }
      }

      //
    }
  }

  if (WRITE_RECORDS) {
    // insert records to the table
    const tabs = tables;
    var total = 0;

    // For each table, generates random records and insert them into the table.
    for (let i = 0; i < tabs.length; i++) {
      const tab = tabs[i];
      if (tab.objtype === "TABLE") {
        logger.info(`==============================================`);
        logger.info(`Inserting into: ${ks}.${tab.keyspace}_${tab.name}`);
        const recs = tab.isCounter ? await generateUpdateRecords(tab, NUM_RECORDS) : await generateRecords(tab, NUM_RECORDS);
        for (let i = 0; i < recs.length; i++) {
          try {
            const rs = await client.execute(
              recs[i],
              {},
              { consistency: types.consistencies.localQuorum }
            );
            total += recs.length;
          } catch (error) {
            logger.error(error);
            break;
          }
        }

        logger.info(
          `Inserted: ${ks}.${tab.keyspace}_${tab.name}: ${recs.length}`
        );
        logger.info(
          `Inserted: ${ks}.${tab.keyspace}_${tab.name} | CQL Sample: ${recs[0]}`
        );

        if (SELECT_LIMIT > 0) {
          const rs = await client.execute(
            await generateSelectRecords(tab),
            {},
            { consistency: types.consistencies.localQuorum }
          );
          tabs[i].rowLengthFromSelect = rs.rowLength
          logger.info(
            `Read: ${ks}.${tab.keyspace}_${tab.name} | CQL Sample: ${recs[0]}`
          );

        }
      }
    }

    logger.info(`Total Records Inserted: ${total}`);

    if (DROP_TABLES) {
      logger.info(`Dropping tables`);
      await dropTables(client);
    }
  }
}

async function generateRecords(tab, recsToGenerate) {
  const recs = [];
  for (let i = 0; i < recsToGenerate; i++) {
    const rec = { cols: [], data: [] };
    for (let j = 0; j < tab.columns.length; j++) {
      const col = tab.columns[j];
      if (dataTypeGenerator[col.colType]) {
        try {
          rec.cols.push(col.name);
          rec.data.push(dataTypeGenerator[col.colType](col.name));
        } catch (error) {
          logger.error(error);
        }
      }
    }
    recs.push(
      `INSERT INTO ${ks}.${tab.keyspace}_${tab.name} (${rec.cols.join(
        ","
      )}) VALUES (${rec.data.join(",")})`
    );
  }
  return recs;
}

async function generateUpdateRecords(tab, recsToGenerate) {
  const recs = [];
  for (let i = 0; i < recsToGenerate; i++) {
    const rec = { cols: [], where: [] };
    for (let j = 0; j < tab.columns.length; j++) {
      const col = tab.columns[j];

      if (dataTypeGenerator[col.colType]) {
        try {
          if (col.isPrimaryKey) {
            rec.where.push(`${col.name} = ${dataTypeGenerator[col.colType](col.name)}`)
          } else if (col.isCounter) {
            rec.cols.push(`${col.name} = ${col.name} + ${dataTypeGenerator[col.colType](col.name)}`)
          }

        } catch (error) {
          logger.error(error);
        }
      }
    }
    recs.push(
      `UPDATE ${ks}.${tab.keyspace}_${tab.name} SET ${rec.cols.join(
        ","
      )} WHERE ${rec.where.join(" AND ")}`
    );
  }
  return recs;
}


async function generateSelectRecords(tab) {
  const stmt =
    `SELECT * FROM ${ks}.${tab.keyspace}_${tab.name} LIMIT ${SELECT_LIMIT}`

  return stmt;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function readSchemas(mode) {
  logger.info(`Running mode: ${mode}`)

  /**
   * Connect to the CQL Proxy locally
   */
  const client = new Client({
    contactPoints: [process.env.ASTRA_PROXY_IP],
    localDataCenter: process.env.ASTRA_DC,
    keyspace: ks,
    credentials: {
      username: process.env.ASTRA_CLIENT_ID,
      password: process.env.ASTRA_CLIENT_SECRET,
    },
  });

  await client.connect();
  logger.info(`Connected`);

  if (mode === 'process') {

    if (DROP_TABLES) {
      // Drop all tables from the app keyspace
      await dropTables(client);
    }

    if (CLEAR_STATS) {
      // Truncate statistics table
      await clearStats(client);
    }

    const schemaFolder = process.cwd() + "/schemas/";
    const files = await readdir(schemaFolder);
    const tables = [];
    for (const file of files) {
      await processSchemaFile(client, schemaFolder + file);
    }
  }

  // collect the results
  await generateResults(client);
  await client.shutdown();
}

async function clearStats(client) {
  try {
    logger.info(`Truncating stats table`);
    await client.execute(`TRUNCATE TABLE ${ks}.astra_usage_stats`);
    await client.execute(`TRUNCATE TABLE ${ks}.astra_usage_histograms`);
  } catch (error) {
    logger.error(error);
    process.exit(1);
  }
}

async function generateResults(client) {
  var rs = await client.execute(
    `select time_bucket, table_ref, 
            update_count, insert_count, writes_size, wrus,
            select_rrus, select_size, select_count
    from ${ks}.astra_usage_stats;`
  );
  const file = await createWriteStream(process.cwd() + "/out/table_stats_from_cql_proxy.csv");
  file.on("error", function (err) {
    logger.error(err)
  });
  file.write(
    `table_ref,keyspace,table,write_count,write_size,write_wrus,WRU per record,RRU per ${SELECT_LIMIT} rows,Rows per RRU` +
    "\n"
  );

  let rows = await sortBy(rs.rows, "table_ref");
  await rows.forEach(async (e) => {
    file.write(
      `${e.table_ref.replace("_", ".").replace(`${ks}.`, "")},` +
      `${e.table_ref
        .substr(0, e.table_ref.indexOf("_"))
        .replace(`${ks}.`, "")},` +
      `${e.table_ref.substr(e.table_ref.indexOf("_") + 1)},` +
      `${max([e.insert_count, e.update_count])},`+
      `${e.writes_size},`+
      `${e.wrus},` +
      `${e.wrus / max([e.insert_count, e.update_count])},` +
      `${e.select_rrus / e.select_count},` +
      `${Math.ceil(RRU_SIZE / (e.select_size / SELECT_LIMIT))}` +
      "\n"
    );
  });
  file.end();
  logger.info("File generated: table_stats_from_cql_proxy")
}

async function dropTables(client) {
  /**
   * Drop Tables, then types
   */
  var rs = await client.execute(
    `select keyspace_name, table_name from system_schema.tables where keyspace_name = '${ks}'`
  );
  for (let i = 0; i < rs.rows.length; i++) {
    if (!AstraUsageTables.includes(rs.rows[i].table_name)) {
      logger.info(`Dropping table: ${ks}.${rs.rows[i].table_name}`);
      try {
        await client.execute(`DROP TABLE ${ks}.${rs.rows[i].table_name}`);
      } catch (error) {
        logger.error(error);
      }
    }
  }

  rs = await client.execute(
    `select keyspace_name, type_name from system_schema.types where keyspace_name = '${ks}'`
  );
  for (let i = 0; i < rs.rows.length; i++) {
    if (!AstraUsageTables.includes(rs.rows[i].table_name)) {
      logger.info(`Dropping type: ${ks}.${rs.rows[i].type_name}`);
      try {
        await client.execute(`DROP TYPE ${ks}.${rs.rows[i].type_name}`);
      } catch (error) {
        logger.error(error);
      }
    }
  }
}

// Run the async function
readSchemas(process.argv[2]);
