import { rmSync, writeFileSync } from "fs";
import { QueryOptions, QueryTypes } from "sequelize";
import { Queue } from "@tiemma/sonic-core";
import metadata from "../../backup/metadata.json";
import { generateRestorePath, getIndependentNodes } from "../graph";
import { Restore, Result } from "../types";
import { isMaster, Map, MapReduce } from "../map_reduce";
import { getDBInstance } from "../models";
import { getLogger } from "../utils";
import config from "../../config.json";
import {
  ensureDependenciesSatisfied,
  getMaxDependencyCount,
  isProcessed,
  readBackup,
  sequelize,
  storageFileName,
  Tables,
} from "./utils";

export const restore = (): Restore => {
  const { tableDependencies, inDegreeMap } = metadata as any;
  const dependencies = Object.keys(tableDependencies);

  for (const node of dependencies) {
    tableDependencies[node] = new Set(tableDependencies[node]);
    if (inDegreeMap[node]) {
      inDegreeMap[node] = new Set(inDegreeMap[node]);
    }
  }

  const queue = getIndependentNodes(tableDependencies);
  if (queue.isEmpty()) {
    throw "Cyclic dependency list";
  }

  const results: Result = {};
  while (!queue.isEmpty()) {
    const node = queue.dequeue();
    if (!results[node]) {
      results[node] = [];
    }

    for (const end of dependencies) {
      if (node === end) continue;

      if (!results[end]) {
        results[end] = [];
      }

      for (const data of generateRestorePath(node, end, inDegreeMap)) {
        results[end].push(data);
      }
    }
  }

  return { metadata: metadata as any, results };
};

export const restoreWorkerTask = async (args, data) => {
  const logger = getLogger(args.workerName);

  const { metadata, config } = args;
  const { table } = data;

  const dbInstance = getDBInstance(config);

  try {
    await ensureDependenciesSatisfied(metadata, table);

    await dbInstance.sequelize.transaction(async (transaction) => {
      const queryOptions: QueryOptions = {
        transaction,
        logging: false,
        benchmark: false,
      };
      await dbInstance.dropTable(table, queryOptions);
      await dbInstance.sequelize.query(readBackup(table), {
        type: QueryTypes.INSERT,
        ...queryOptions,
      });
      await Tables.update(
        { isProcessed: true },
        {
          where: {
            name: table,
            isProcessed: false,
          },
        }
      );
    });
  } catch (err) {
    logger(`Error processing table ${table}: ${err}`);
  } finally {
    await dbInstance.sequelize.close();
  }

  return table;
};

export const restoreMasterTask = async (args: any, workerQueue: Queue) => {
  const { adjMatrix } = args;

  writeFileSync(
    `${process.cwd()}/adjMatric.json`,
    JSON.stringify(adjMatrix, null, "\t")
  );

  await sequelize.authenticate({ benchmark: true });

  await Tables.sync({ force: true, logging: true });

  for (const [root, dependencies] of Object.entries(adjMatrix as Result)) {
    for (let i = 0; i < getMaxDependencyCount(dependencies); i++) {
      for (let j = 0; j < dependencies.length; j++) {
        if (
          i < dependencies[j].length &&
          !(await isProcessed(dependencies[j][i]))
        ) {
          await Map(workerQueue, { data: { table: dependencies[j][i] } });
        }
      }
    }

    if (!(await isProcessed(root))) {
      await Map(workerQueue, { data: { table: root } });
    }
  }

  rmSync(`${storageFileName}`);
};

(async () => {
  const { metadata, results: adjMatrix } = restore();

  const numWorkers = 4;

  console.log(
    await MapReduce(
      restoreMasterTask,
      restoreWorkerTask,
      (queue: Queue) => queue.getElements().map((x) => x.data),
      {
        metadata,
        adjMatrix,
        config,
        numWorkers,
      }
    )
  );
})();
