import { rmSync } from "fs";
import { Queue } from "@tiemma/sonic-core";
import { Map, MapReduceEvent } from "@tiemma/sonic-distribute";
import { generateRestorePath, getIndependentNodes } from "../graph";
import { Restore, Result } from "../types";
import { getDBInstance } from "../models";
import {
  backupDir,
  backupMetadata,
  ensureDependenciesSatisfied,
  getMaxDependencyCount,
  isProcessed,
  MetadataFiles,
  sequelize,
  storageFileName,
  Tables,
} from "./utils";

const generateAdjacencyMatrix = (metadataName: MetadataFiles): Restore => {
  const {
    tableDependencies,
    inDegreeMap,
  } = require(`${backupDir}/${metadataName}`);
  const dependencies = Object.keys(tableDependencies);

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

  return { metadata: { tableDependencies, inDegreeMap }, results };
};

const workerFn = async (event: MapReduceEvent, args: any) => {
  const { config } = args;
  const { table, metadata } = event.data;

  const dbInstance = getDBInstance(config);

  try {
    await ensureDependenciesSatisfied(metadata, table);

    await dbInstance.executeBackupQuery(table);
    await Tables.update(
      { isProcessed: true },
      {
        where: {
          name: table,
          isProcessed: false,
        },
      }
    );
  } catch (err) {
    await dbInstance.sequelize.close();

    throw err;
  }

  return table;
};

const masterFn = async (workerQueue: Queue, _: any) => {
  const { results: adjMatrix, metadata } = generateAdjacencyMatrix(
    MetadataFiles.METADATA_GRAPH
  );

  backupMetadata(backupDir, MetadataFiles.ADJACENCY_MATRIX, adjMatrix);

  await sequelize.authenticate({ benchmark: true });

  await Tables.sync({ force: true, logging: true });

  for (const [root, dependencies] of Object.entries(adjMatrix)) {
    for (let i = 0; i < getMaxDependencyCount(dependencies); i++) {
      for (let j = 0; j < dependencies.length; j++) {
        if (
          i < dependencies[j].length &&
          !(await isProcessed(dependencies[j][i]))
        ) {
          await Map(workerQueue, {
            data: { table: dependencies[j][i], metadata },
          });
        }
      }
    }

    if (!(await isProcessed(root))) {
      await Map(workerQueue, { data: { table: root, metadata } });
    }
  }
};

const reduceFn = (processQueue: Queue, failedQueue: Queue) => {
  rmSync(`${storageFileName}`);

  return `Processed ${
    processQueue.getElements().length
  } tables and failed for ${
    failedQueue.getElements().length
  } table(s): ${failedQueue.getElements().map((x) => x.data.table)}`;
};

export const restoreOpsMap = {
  masterFn,
  workerFns: [workerFn],
  reduceFn,
};
