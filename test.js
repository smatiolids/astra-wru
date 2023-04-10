import lodash from 'lodash';
const { random, isString, uniq } = lodash;
import { AVERAGE_SET_LENGTH, advDataTypeGenerator, dataTypeGenerator } from "./consts.js"

async function AdvDT(name, type) {
    const first = type.indexOf('<')
    if (first > 0) {
        const advType = type.substr(0, first)
        const innerType = type.substring(first + 1, type.length - 1)

        if (advType === 'MAP') {
            const elems = innerType.split(",").map(e => e.trim())
            const len = random(AVERAGE_SET_LENGTH[0], AVERAGE_SET_LENGTH[1])
            let res = {}
            for (let i = 0; i < len; i++) {
                let k = await AdvDT(name, elems[0])
                let v = await AdvDT(name, elems[1])
                if (isString(k)) k = 'k' + k.replace(/'/g, "")
                if (isString(v)) v = 'v' + v.replace(/'/g, "")
                res[k] = v
            }
            return res
        }
        else if (advType === 'SET' || advType === 'LIST') {
            const len = random(AVERAGE_SET_LENGTH[0], AVERAGE_SET_LENGTH[1])
            const res = []
            for (let i = 0; i < len; i++) {
                let v = await AdvDT(name, innerType)
                res.push(v)
            }
            return advType === 'SET' ? `{${uniq(res).join(',')}}` : `[ ${res.join(',')} ]`
        }
        else if (advType === 'TUPLE') {
            const res = await await Promise.all(innerType.split(',').map(async e => await AdvDT(name, e.trim())))
            return `( ${res.join(',')} )`
        }
        else return await AdvDT(name, innerType)
    } else {
        if (dataTypeGenerator[type])
            return dataTypeGenerator[type](name)
    }

    throw new Error(`Invalid type: ${type}`)
}

async function run() {
    // console.log(await AdvDT({ name: 'solr_query', 'TEXT' }))

    // console.log(await AdvDT('txt_dici_bsca_rotr_jorn_clie_', 'map<text, text>'.toUpperCase()))
    // console.log(await AdvDT('txt_dici_bsca_rotr_jorn_clie_', 'map<text, int>'.toUpperCase()))
    // console.log(await AdvDT('txt_list_decs_prox_atii', 'map<text, uuid>'.toUpperCase()))
    // console.log(await AdvDT('set_field', 'set<text>'.toUpperCase()))
    // console.log(await AdvDT('set_field', 'set<int>'.toUpperCase()))
    // console.log(await AdvDT('list_field', 'list<text>'.toUpperCase()))
    // console.log(await AdvDT('list_field', 'list<int>'.toUpperCase()))
    // console.log(await AdvDT('tuple_field', 'tuple<timeuuid, timestamp>'.toUpperCase()))
    // console.log(await AdvDT('tuple_and set_field', 'set<frozen<tuple<timeuuid, timestamp>>>'.toUpperCase()))
    //console.log(await AdvDT('tuple_and list_field', 'frozen<list<frozen<tuple<text, text>>>>'.toUpperCase()))
    console.log(await AdvDT('', 'frozen<sd9cas001.tpsd9_tipo_perg_usua>'.toUpperCase()))
    


}

run()



INSERT INTO app.SD9CAS001_TBSD9001_CADA_ROTR_JORN_CLIE (COD_TNNT,NOM_SITU_ROTR_JORN_CLIE,COD_ROTR_JORN_CLIE,NOM_ROTR_JORN_CLIE,NOM_TNNT,SOLR_QUERY,TXT_DES_ROTR_JORN) 
VALUES (a2795c6a-c2ed-4115-95b4-c5638504cbb2,'ffbfd2d9564e9de3b3',ff04ab9b-c4d0-4bb1-8e95-f1cc1a4b90d9,'94a02b18d8909653ffe1','b6db48c5f9','8865e53d630e883b2ee02178f0783f51b306','b6fce1b939da6080')","service":"user-service","timestamp":"2023-04-08T22:01:22.572Z"}


{"level":"info","message":"Read: app.SD9CAS001_TBSD9001_CADA_ROTR_JORN_CLIE | CQL Sample: 

INSERT INTO app.SD9CAS001_TBSD9001_CADA_ROTR_JORN_CLIE (
    COD_TNNT,NOM_SITU_ROTR_JORN_CLIE,COD_ROTR_JORN_CLIE,NOM_ROTR_JORN_CLIE,NOM_TNNT,SOLR_QUERY,TXT_DES_ROTR_JORN,TXT_DICI_BSCA_ROTR_JORN_CLIE_) 
    VALUES (8e325376-4021-4785-bf3e-b6d5f4b156cb,'7154086d2b630ffd832e5e612d322e90e3',cbd70013-24b0-414a-aa97-51fba0823e54,'6cc3','b74bd7afd03a',
    '173f54cd67d3164873','620fb5274c',
    {\"k2649443552bc\":\"vb772fc51698ccad492c192\",\"kd6137c57b7\":\"v24be2d070149c380f481ccace525\",
    \"kc3bf0f3d792751c438d7621294a2745918fe\":\"v4bfc0521194c93f324\",\"k675c9089\":\"ve7ff7a3f\",
    \"k4a5bf34ef311596e3d74f540d2556107c68a\":\"v02cf81a73f7cde7097da30f45f1085e9b6ebc9\",
    \"k32ee15b535efed000fc0f7893bab48\":\"v8721f3fc298ffa\",\"k4637feb507c289fe\":\"vf7857f69ac7164cbd46ad146a6cc19de9ac76d\",
    \"kc3e9cd\":\"v7f8289014dcb0612d9d2c07196a6212d58\",\"kacc3f05d769639dad2eb0f26\":\"vb91df0cfde41\",
    \"k2697081c5431175b4d753c\":\"v9bf43bb0c9016449f3c73426b131\"})


INSERT INTO app.SD9CAS001_TBSD9001_CADA_ROTR_JORN_CLIE (
    COD_TNNT,NOM_SITU_ROTR_JORN_CLIE,COD_ROTR_JORN_CLIE,NOM_ROTR_JORN_CLIE,NOM_TNNT,SOLR_QUERY,TXT_ASSC_PRVD_INTL_ARTL,TXT_DES_ROTR_JORN,TXT_DICI_BSCA_ROTR_JORN_CLIE_) 
VALUES (
    bb8a5270-ea3d-4c9a-8ebc-551f275735a9,
    '6b88d5e9b8',
    87d07f85-5985-4484-abf6-e2186e10c926,
    '7b22bc71b45740da79512baeffe2',
    'b918ba',
    'f8594509eb869359b965a38e09c6',
    [ {NOM_PRVD_INTL_ARTL:'v3ff9826ea62770472a6c',NOM_AGET_INTL_ARTL:'vd7240c12efc2428da0a29fd9',COD_ASSC_GATL_ROTR:'v69d68e0e7adf76cd3f36'},
    {NOM_PRVD_INTL_ARTL:'v6fd78cb6b46f2b437ed0a6',NOM_AGET_INTL_ARTL:'ve6ae0cda70c89a7cbdab45b6',COD_ASSC_GATL_ROTR:'v4b7c62f57f081f9257d8'},
    {NOM_PRVD_INTL_ARTL:'v423676',NOM_AGET_INTL_ARTL:'vfa577fc6b324',COD_ASSC_GATL_ROTR:'v6632789e5c9ea91a0a7e'},
    {NOM_PRVD_INTL_ARTL:'v3aab39c96bbd8b3a9e17ca',NOM_AGET_INTL_ARTL:'vda7f379fd1c0114418',COD_ASSC_GATL_ROTR:'v5a4b6a13499e5b1ae9e2'} ],
    '653a09cbb9cc002f733d98fe90e1e22b',
    {'k1e5616dbabcdc5ace8e099d2471ea1f79bc970a0':'v3648fa7e534023','k8a2a3a96b9086c1fb37fe6f3d1aa96':'vdac55b15f178696e33e2bee4ede22749f467'}
    )
