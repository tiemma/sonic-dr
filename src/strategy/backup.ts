import { Map, MapReduceEvent } from "@tiemma/sonic-distribute";
import { Queue } from "@tiemma/sonic-core";
import { DBMetadataGraph } from "../types";
import { getDBInstance } from "../models";
import { convertIntoGraphAndSort } from "../graph";
import { getLogger } from "../utils";
import { backupDir, backupMetadata, MetadataFiles, promiseExec } from "./utils";

const logger = getLogger("BACKUP");

const reduceFn = async (queue: Queue, failed: Queue) => {
  return `Backed up ${queue.getElements().length} tables and failed for ${
    failed.getElements().length
  } table(s): ${failed.getElements().map((x) => x.data.table)}`;
};

const workerFn = async (event: MapReduceEvent, args: any) => {
  const { table } = event.data;
  const { config, tableSuffixes } = args;
  const dbInstance = getDBInstance(config);
  await dbInstance.writeTableSchema(table);
  await dbInstance.formatRowInserts(table, tableSuffixes[table]);

  return table;
};

const masterFn = async (workerQueue: Queue, args: any) => {
  const { config } = args;
  const dbInstance = getDBInstance(config);

  const [tableDependencies, inDegreeMap] = await Promise.all([
    dbInstance.getDBMetadata(),
    dbInstance.getDBInDegreeMap(),
  ]);

  const queue = convertIntoGraphAndSort(tableDependencies);

  backupMetadata(backupDir, MetadataFiles.METADATA_GRAPH, {
    tableDependencies,
    inDegreeMap,
  } as DBMetadataGraph);

  while (!queue.isEmpty()) {
    const table = queue.dequeue();

    logger(`Processing ${table}`);

    await Map(workerQueue, { data: { table } });
  }
};

export const backupOpsMap = {
  masterFn,
  workerFns: [workerFn],
  reduceFn,
};
