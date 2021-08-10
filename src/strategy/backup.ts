import { exec } from "child_process";
import { Options } from "sequelize";
import { DBMetadataGraph } from "../types";
import { getDBInstance } from "../models";
import { convertIntoGraphAndSort } from "../graph";
import { getLogger } from "../utils";
import { backupDir, backupMetadata } from "./utils";

const logger = getLogger("BACKUP");

export const backup = async (config: Options) => {
  const dbInstance = getDBInstance(config);

  const [tableDependencies, inDegreeMap] = await Promise.all([
    dbInstance.getDBMetadata(),
    dbInstance.getDBInDegreeMap(),
  ]);
  const queue = convertIntoGraphAndSort(tableDependencies);

  backupMetadata(backupDir, {
    tableDependencies,
    inDegreeMap,
  } as DBMetadataGraph);

  while (!queue.isEmpty()) {
    const table = queue.dequeue();

    logger(`Processing ${table}`);

    new Promise<void>((resolve, reject) => {
      exec(
        dbInstance.getBackupCommand(config, table),
        (error, stdout, stderr) => {
          if (stdout) {
            logger(`stdout: $;{stdout}`);
          }

          if (error) {
            logger(`exec error: ${error}`);
            reject();

            return;
          }

          if (stderr) {
            logger(`stderr: ${stderr}`);
            reject();
          }

          resolve();
        }
      );
    });
  }
};

// backup(config as Options);
