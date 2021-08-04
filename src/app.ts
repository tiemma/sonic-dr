import {Postgres} from "./models";
import {convertIntoGraphAndSort} from "./graph/parse";
import {exec} from "child_process"
import config from "../config.json";
import {Options} from "sequelize";
import {mkdirSync, existsSync} from "fs";

(async () => {
    const foreignKeyRelationships = await Postgres.init(config as Options).getPostgresDBMetadata()
    const queue = convertIntoGraphAndSort(foreignKeyRelationships);

    const backupDir = `${process.cwd()}/backup`
    if (!existsSync(backupDir)) {
        mkdirSync(backupDir);
    }

    while(!queue.isEmpty()){
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

                if(stderr) {
                    console.error(`stderr: ${stderr}`);
                    reject()
                }

                resolve();
            });
        })

    }
})()