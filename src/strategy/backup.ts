import {DBMetadataGraph} from "../types";
import {getDBInstance} from "../models";
import {Options} from "sequelize";
import {convertIntoGraphAndSort} from "../graph";
import {exec} from "child_process";

import config from "../../config.json"

const backupMetadata = (data: DBMetadataGraph) => {
    require("fs").writeFileSync("./metadata.json", JSON.stringify(data, null, "\t"))
}

export const backup = async (config: Options) => {
    const dbInstance = getDBInstance(config)

    const [tableDependencies, inDegreeMap] = await Promise.all([dbInstance.getPostgresDBMetadata(), dbInstance.getPostgresInDegreeMap()]);
    const queue = convertIntoGraphAndSort(tableDependencies);

    require("fs").writeFileSync("./order", queue.getElements().join(","))

    while (!queue.isEmpty()) {
        const table = queue.dequeue();

        console.log(`Processing ${table}`)

        backupMetadata({tableDependencies, inDegreeMap} as DBMetadataGraph)

        new Promise<void>((resolve, reject) => {
            exec(dbInstance.getBackupCommand(config, table), (error, stdout, stderr) => {
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

// backup(config as Options)