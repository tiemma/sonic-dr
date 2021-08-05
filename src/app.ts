import {Postgres} from "./models";
import {convertIntoGraphAndSort, generateRestorePath, getIndependentNodes} from "./graph/parse";
import {exec} from "child_process"
import config from "../config.json";
import {Options} from "sequelize";
import {mkdirSync, existsSync} from "fs";
import {Result, Scheduler} from "./types";
import {Queue} from "@tiemma/sonic-core";


const psInstance = Postgres.init(config as Options)

const postgresRestore = async () => {
    const postgresDBMetadata = require(`${process.cwd()}/metadata.json`)
    const indegreeeMap = await psInstance.getPostgresInDegreeMap()
    for (const node of Object.keys(postgresDBMetadata)) {
        postgresDBMetadata[node] = new Set(postgresDBMetadata[node])
        if (indegreeeMap[node]) {
            indegreeeMap[node] = new Set(indegreeeMap[node])
        }
    }

    const queue = getIndependentNodes(postgresDBMetadata)
    if (queue.isEmpty()) {
        throw "Cyclic dependency list"
    }

    const scheduler: Scheduler = {
        processNow: queue,
        processLater: new Queue(),
    }

    const results: Result = {}
    while (!queue.isEmpty()) {
        const node = queue.dequeue()
        for (const end of Object.keys(postgresDBMetadata)) {
            if (node != end) {
                for (const data of generateRestorePath(node, end, indegreeeMap)) {
                    if(!results[end]){
                        results[end] = [];
                    }
                    data.pop()
                    results[end].push(data)
                }
            }
        }

    }

    console.log(results)

}


const postgresBackup = async () => {
    const foreignKeyRelationships = await psInstance.getPostgresDBMetadata()
    require("fs").writeFileSync("./metadata.json", JSON.stringify(foreignKeyRelationships, null, "\t"))
    const queue = convertIntoGraphAndSort(foreignKeyRelationships);

    const backupDir = `${process.cwd()}/backup`
    if (!existsSync(backupDir)) {
        mkdirSync(backupDir);
    }

    while (!queue.isEmpty()) {
        const table = queue.dequeue();

        console.log(`Processing ${table}`)

        new Promise<void>((resolve, reject) => {
            exec(`
        PGPASSWORD=${config.password} \
        pg_dump -h ${config.host} -d ${config.database}  -U ${config.username} \
        -f "${backupDir}/${table}.sql" \
        -O -t ${table} \
        --no-tablespaces \
         -x  -c \
        --column-inserts \
         --format=plain \
         --inserts \
         --if-exists \
          --no-publications --no-security-labels --no-subscriptions --no-synchronized-snapshots --no-unlogged-table-data --quote-all-identifiers 
        `, (error, stdout, stderr) => {
                if (stdout) {
                    console.log(`stdout: ${stdout}`);
                }

                if (error) {
                    console.error(`exec error: ${error}`);
                    reject();
                    return;
                }

                if (stderr) {
                    console.error(`stderr: ${stderr}`);
                    reject()
                }

                resolve();
            });
        })

    }
}

(async () => await postgresRestore())();
