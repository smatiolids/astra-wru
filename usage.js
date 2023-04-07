import { readdir } from "fs/promises";
import { basename, resolve } from 'path';
import { readFile } from "fs/promises";
import { keyspacesToIgnore } from "./consts.js";

const IP_FORMAT = /\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/

async function getFiles(dir) {
    const dirents = await readdir(dir, { withFileTypes: true });
    const files = await Promise.all(dirents.map((dirent) => {
        const res = resolve(dir, dirent.name);
        return dirent.isDirectory() ? getFiles(res) : res;
    }));
    return Array.prototype.concat(...files);
}


async function readNodetool(basedir) {

    // Read all files and filter the important ones
    const files = await getFiles(basedir);
    const nodefiles = files.filter(e => ['/info', '/cfstats'].some(f => e.endsWith(f)))

    // Group files by node
    const nodes = nodefiles.reduce((acc, cur) => {
        const ip = cur.match(IP_FORMAT)
        const file = basename(cur)

        if (ip && ip[0]) {
            if (acc[ip[0]]) {
                acc[ip[0]].files[file] = cur
            } else {
                acc[ip[0]] = {
                    files: {
                        [file]: cur
                    }
                }
            }
        }
        return acc
    }, {})

    const tables = {}

    for (const node of Object.keys(nodes)) {
        nodes[node].info = await readInfo(nodes[node].files.info)
        nodes[node].cfstats = await readCfStats(nodes[node].files.cfstats)
    }

    // console.log(nodefiles)
    console.log(nodes)

}


async function readInfo(file) {
    const data = await readFile(file);
    const items = data.toString().split(/\r?\n|\r|\n/gm)
    const res = {}
    for (let item of items) {
        item = item.replace(/\t/g, "")
        if (item === "") continue
        const detail = item.split(":").map(e => e.trim())
        res[detail[0]] = normalizeValue(detail[1])
    }
    console.log(res)
    return res
}

async function readCfStats(file) {
    const data = await readFile(file);
    const items = data.toString().split(/\r?\n|\r|\n/gm)
    const last = {}
    const res = {}



    for (let item of items) {
        item = item.replace(/\t/g, "")
        if (item === "") continue
        const detail = item.split(":").map(e => e.trim())

        if (detail[0] === "Keyspace") {
            last.block = 'Keyspace'
            last.keyspace = detail[1]
            res[detail[1]] = {
                tables: {}
            }
            continue
        }

        if (detail[0] === "Table") {
            last.block = 'Table'
            last.table = detail[1]
            res[last.keyspace].tables[detail[1]] = {}
            continue
        }

        if (last.block === 'Keyspace') {
            res[last.keyspace][detail[0]] = normalizeValue(detail[1])
        }

        if (last.block === 'Table') {
            res[last.keyspace].tables[last.table][detail[0]] = normalizeValue(detail[1])
        }
    }


    for (let key of keyspacesToIgnore){
        delete res[key]
    }

    return res
}

function normalizeValue(value) {
    const floatValue = parseFloat(value);
    if (isNaN(floatValue)) {
        return value;
    } else {
        return floatValue;
    }
}
readNodetool(process.argv[2])

// readCfStats('/Users/samuel.matioli/work/customers/itau/2023/AVI_PROD_CLUSTER_EM4-diagnostics-2023_02_03_14_19_56_UTC/nodes/10.58.50.95/nodetool/cfstats')
// readInfo('/Users/samuel.matioli/work/customers/itau/2023/AVI_PROD_CLUSTER_EM4-diagnostics-2023_02_03_14_19_56_UTC/nodes/10.58.50.95/nodetool/info')