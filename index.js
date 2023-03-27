import dotenv from "dotenv";
import { Client, types } from "cassandra-driver";
import { processSchema } from "./schema.js";
import { AstraUsageTables, dataTypeGenerator } from "./consts.js";
import { readdir } from "fs/promises";
import { createLogger, format, transports } from "winston";
import { createWriteStream } from "fs";
import _ from "lodash";

const ks = "app";
const DROP_TABLES = false;
const CREATE_TABLES = false;
const WRITE_RECORDS = false;
const CLEAR_STATS = false;
const NUM_RECORDS = 100;
const DC = "sa-east-1";
const RRU_SIZE = 4000;
const ROWS_PER_READ = 10;

const logger = createLogger({
  level: "info",
  format: format.json(),
  defaultMeta: { service: "user-service" },
  format: format.combine(format.timestamp(), format.json()),
  transports: [
    //
    // - Write all logs with importance level of `error` or less to `error.log`
    // - Write all logs with importance level of `info` or less to `combined.log`
    //
    new transports.Console(),
    new transports.File({
      filename: "./out/error.log",
      level: "error",
    }),
    new transports.File({ filename: "./out/combined.log" }),
  ],
});

dotenv.config();

async function processSchemaFile(client, file) {
  logger.info(`Starting reading ${file}`);

  // Extract tables from schema file
  const tables = await processSchema(file);

  if (CREATE_TABLES) {
    const errors = [];
    for (let i = 0; i < tables.length; i++) {
      const tab = tables[i];
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
        const recs = await generateRecords(tab, NUM_RECORDS);
        for (let i = 0; i < recs.length; i++) {
          try {
            const rs = await client.execute(
              recs[i],
              {},
              { consistency: types.consistencies.localQuorum }
            );
            total += recs.length;
            // await sleep(1000);
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

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function readSchemas(e) {
  const client = new Client({
    contactPoints: ["127.0.0.1"],
    localDataCenter: DC,
    keyspace: ks,
    credentials: {
      username: process.env.ASTRA_CLIENT_ID,
      password: process.env.ASTRA_CLIENT_SECRET,
    },
  });

  // const client = new Client({
  //   cloud: {
  //     secureConnectBundle: "../secure-connect-astra-aws-sp.zip",
  //   },
  //   credentials: {
  //     username: process.env.ASTRA_CLIENT_ID,
  //     password: process.env.ASTRA_CLIENT_SECRET,
  //   },
  // });

  await client.connect();
  logger.info(`Connected`);

  if (DROP_TABLES) {
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
    `select time_bucket, table_ref, insert_count, insert_size, 
    insert_wrus, writes_size, wrus
    from ${ks}.astra_usage_stats;`
  );
  const file = await createWriteStream(process.cwd() + "/out/table_stats.csv");
  file.on("error", function (err) {
    /* error handling */
  });
  file.write(
    `table_ref,keyspace,table,insert_count,insert_size,insert_wrus,writes_size,WRU per record,Rows per RRU,RRU per ${ROWS_PER_READ} rows` +
      "\n"
  );

  let rows = await _.sortBy(rs.rows, "table_ref");
  await rows.forEach(async (e) => {
    file.write(
      `${e.table_ref.replace("_", ".").replace(`${ks}.`, "")},` +
        `${e.table_ref
          .substr(0, e.table_ref.indexOf("_"))
          .replace(`${ks}.`, "")},` +
        `${e.table_ref.substr(e.table_ref.indexOf("_") + 1)},` +
        `${e.insert_count},${e.insert_size},${e.insert_wrus},` +
        `${e.writes_size},${e.wrus / e.insert_count},` +
        `${Math.ceil(RRU_SIZE / (e.insert_size / e.insert_count))},` +
        `${Math.ceil(
          ROWS_PER_READ / Math.ceil(RRU_SIZE / (e.insert_size / e.insert_count))
        )}` +
        "\n"
    );
  });
  file.end();
}

async function dropTables(client) {
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
readSchemas();
