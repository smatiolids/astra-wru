import fs from "fs";
import _ from "lodash";
import {
  colType,
  colTypeSpecial,
  dataTypeSizes,
  AVERAGE_LIST_LENGTH,
  AVERAGE_SET_LENGTH,
  keyspacesToIgnore,
} from "./consts.js";
import { readFile, readdir } from "fs/promises";

const objTypes = {
  TYPE: 0,
  "MATERIALIZED VIEW": 0,
  TABLE: 0,
  KEYSPACE: 0,
  INDEX: 0,
  "CUSTOM INDEX": 0,
};

function regexIndexOf(text, re, i = 0) {
  var indexInSuffix = text.slice(i).search(re);
  return indexInSuffix < 0 ? indexInSuffix : indexInSuffix + i;
}

async function parseObject(cql) {
  // console.log(regexIndexOf(cql, /(\sWITH|\sAS\s|\(|\sON\s)/gm));
  var str = cql
    .substring(0, regexIndexOf(cql, /(\sWITH|\sAS\s|\(|\sON\s)/gm))
    .replace("CREATE", "")
    .trim();

  const obj = {};
  try {
    obj.objtype = Object.keys(objTypes)
      .find((e) => str.includes(e))
      .toUpperCase();
    str = str.replace(obj.objtype, "").trim().toUpperCase();
    obj.keyspace = str.split(".")[0];
    obj.name = str.split(".")[1] ? str.split(".")[1] : null;
    // console.log(obj);
    return obj;
  } catch (error) {
    return null;
  }
}

async function parseTableAndType(dataTypes, cql) {
  var str = cql
    .trim()
    .toUpperCase()
    .replace(/(?:\r\n|\r|\n)/g, " ")
    .replace(/\s+/g, " ")
    .replace(/(<.*?>)/g, function (match) {
      return match.replace(/ /g, "");
    });

  var items = str.split(" ");
  const result = {
    objtype: items[1],
    keyspace: items[2].split(".")[0],
    name: items[2].split(".")[1],
    size: 0,
    columns: [],
  };

  result.colDefinition = cql
    .slice(cql.indexOf("("), cql.indexOf(" AND ") + 1)
    .toUpperCase();

  if (result.objtype === "TYPE") {
    result.colDefinition = cql.slice(cql.indexOf("(")).toUpperCase();
  } else if (
    result.colDefinition.includes("WITH") &&
    !result.colDefinition.includes("CLUSTERING")
  ) {
    result.colDefinition = result.colDefinition.slice(
      0,
      result.colDefinition.indexOf(" WITH ")
    );
  }

  result.colDefinition = result.colDefinition.replace(
    new RegExp(`${result.keyspace}.`, "gm"),
    `${process.env.ASTRA_KEYSPACE}.${result.keyspace}_`
  );

  let block = "COL";
  for (let ix = 4; ix < items.length; ix++) {
    let item = items[ix];

    if (item.endsWith(",")) item = item.slice(0, -1);

    if (item === "PRIMARY") block = "PK";
    if (item === "CLUSTERING") block = "CK";

    if (block === "PK") {
      // console.log("pk item", item)
      let col = item.replace(/\(|\)/g, "")
      let cix = result.columns.findIndex(e => e.name === col)
      if (cix >= 0)
        result.columns[cix].isPrimaryKey = true

    }


    if (block === "COL") {
      if (colType.some((dt) => item.startsWith(dt))) {
        result.columns[result.columns.length - 1].definition = item;

        if (item.includes("COUNTER")) {
          result.isCounter = true;
          result.columns[result.columns.length - 1].isCounter = true
        }

        colTypeSpecial.forEach((e) => {
          if (item.includes(`${e}<`)) {
            result.columns[result.columns.length - 1][
              `is${e.charAt(0) + e.slice(1).toLowerCase()}`
            ] = true;

            item = item.replace(`${e}<`, "");
          }
        });

        item = item.replace(/(<|>)/g, "");
        item = item.replace("FROZEN", "");

        result.columns[result.columns.length - 1].colType = item;
        result.columns[result.columns.length - 1].size = 0;
        result.columns[result.columns.length - 1].colTypes = item
          .split(",")
          .map((e) => {
            result.columns[result.columns.length - 1].size += dataTypes[e];
            return {
              t: e,
              size: dataTypes[e],
            };
          });

        if (result.columns[result.columns.length - 1].isSet)
          result.columns[result.columns.length - 1].size =
            result.columns[result.columns.length - 1].size * AVERAGE_SET_LENGTH[1];

        if (result.columns[result.columns.length - 1].isList)
          result.columns[result.columns.length - 1].size =
            result.columns[result.columns.length - 1].size *
            AVERAGE_LIST_LENGTH[1];
      } else if ([")"].indexOf(item) < 0) {
        result.columns.push({ name: item });
      }
    }
  }

  // Summarizes the metrics for the table
  result.size =
    result.columns.reduce((acc, cur) => {
      acc += cur.size;
      return acc;
    }, 0);

  result.WRUPerIRec = Math.ceil(result.size / 1000);
  result.rowsPerWRU = Math.trunc(1000 / result.size);
  result.rowsPerRRU = Math.trunc(4000 / result.size);
  result.rowsSizeWarning = result.rowsPerWRU <= 1;

  return result;
}

export async function processSchema(file) {
  console.log(`Reading file: ${file}`);
  const data = await readFile(file);
  const items = data.toString().split(";");
  const dataTypes = { ...dataTypeSizes };
  const objects = [];

  for (const item of items) {
    const ignoredKS = keyspacesToIgnore.find((i) =>
      item
        .substring(0, item.indexOf("."))
        .toUpperCase()
        .includes(`${i.toUpperCase()}`)
    );

    if (ignoredKS) {
      // console.log(`Ignoring keyspace: ${ignoredKS}`);
      continue;
    }

    if (item.trim().startsWith("CREATE TABLE")) {
      const obj = await parseTableAndType(dataTypes, item);
      objects.push(obj);
      objTypes["TABLE"]++;
    } else if (item.trim().startsWith("CREATE TYPE")) {
      const obj = await parseTableAndType(dataTypes, item);
      dataTypes[`${obj.keyspace}.${obj.name}`] = obj.size;
      objects.push(obj);
      objTypes["TYPE"]++;
    } else {
      const obj = await parseObject(item);
      if (obj) {
        objects.push(obj);
        objTypes[obj.objtype]++;
      }
    }
  }
  return objects;
}

async function readSchemas(write = false) {
  const schemaFolder = process.cwd() + "/schemas/";
  const files = await readdir(schemaFolder);
  const tables = [];
  for (const file of files) {
    (await processSchema(schemaFolder + file)).forEach((e) => tables.push(e));
  }

  if (write) {
    const file = await fs.createWriteStream(process.cwd() + "/out/table_stats_from_schema.csv");
    file.on("error", function (err) {
      /* error handling */
    });
    file.write(
      `objtype,keyspace,name,size,WRUPerRec,rowsPerWRU,rowsPerRRU,rowsSizeWarning,columns` +
      "\n"
    );
    await tables.forEach(async (e) => {
      file.write(
        `${e.objtype},${e.keyspace},${e.name},${e.size || ""},${e.WRUPerIRec || ""
        },${e.rowsPerWRU || ""},${e.rowsPerRRU || ""},${e.rowsSizeWarning || ""
        },${e.columns ? e.columns.length : ""}` + "\n"
      );
    });
    file.end();
  } else {
    // console.log(tables);
  }

}

readSchemas(true);
