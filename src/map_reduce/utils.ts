import { cpus } from "os";
import cluster, { ClusterSettings } from "cluster";
import { rmSync, writeFileSync } from "fs";
import { DataTypes, Sequelize } from "sequelize";
import { Queue } from "@tiemma/sonic-core";
import { DBMetadataGraph, Result } from "../types";
import { Delay, getLogger } from "../utils";

export const NUM_CPUS = cpus().length;

export const clusterEvents = {
  MESSAGE: "message",
  DISCONNECT: "disconnect",
};

export const storageFileName = `${process.cwd()}/db`;

export const sequelize = new Sequelize({
  dialect: "sqlite",
  storage: storageFileName,
  benchmark: false,
  logging: false,
});

export const Tables = sequelize.define(
  "tables",
  {
    name: {
      type: DataTypes.STRING,
      unique: true,
      primaryKey: true,
    },
    isProcessed: {
      type: DataTypes.BOOLEAN,
    },
  },
  {
    timestamps: false,
  }
);

export const isMaster = () => {
  return cluster.isMaster || cluster.isPrimary;
};

export const getLoggerID = () => {
  if (isMaster()) {
    return "MASTER";
  }

  return `WORKER-${cluster.worker.id}`;
};

export const getCount = (metadata: DBMetadataGraph, table: string) => {
  return Tables.sequelize.query(
    `SELECT COUNT(*) AS count FROM tables WHERE name IN (:names) AND isProcessed IS TRUE`,
    {
      replacements: {
        names: Array.from(metadata.tableDependencies[table]),
      },
      plain: true,
    }
  );
};

export const ensureDependenciesSatisfied = async (
  metadata: DBMetadataGraph,
  table: string
) => {
  const dependencySize = metadata.tableDependencies[table].size;
  if (dependencySize === 0) {
    return;
  }

  let filledTables = await getCount(metadata, table);
  while (filledTables["count"] !== dependencySize) {
    filledTables = await getCount(metadata, table);
  }
  getLogger(getLoggerID())(
    `Dependencies fulfilled for table ${table}, expected=${dependencySize}, found=${filledTables["count"]}`
  );
};

export const getWorkerID = async (workerQueue: Queue) => {
  while (workerQueue.isEmpty()) {
    await Delay();
  }

  let workerID = workerQueue.dequeue();
  while (!cluster.workers[workerID]) {
    workerID = workerQueue.dequeue();
  }

  return workerID;
};

export const isProcessed = async (table: string) => {
  const count = await Tables.count({ where: { name: table } });

  if (!count) {
    getLogger(getLoggerID())(`Unprocessed ${table} found`);
    await Tables.create({ name: table, isProcessed: false });

    return false;
  }

  return true;
};

export const getMaxDependencyCount = (dependencies: string[][]) => {
  let max = 0;
  for (const tableSet of dependencies) {
    max = Math.max(max, tableSet.length);
  }

  return max;
};

export const initMaster = async (adjMatrix: Result) => {
  writeFileSync(
    `${process.cwd()}/adjMatric.json`,
    JSON.stringify(adjMatrix, null, "\t")
  );

  await sequelize.authenticate({ benchmark: true });

  await Tables.sync({ force: true, logging: true });

  (cluster.setupMaster || cluster.setupPrimary)({
    execArgv: [
      "-r",
      "tsconfig-paths/register",
      "-r",
      "ts-node/register",
      "--async-stack-traces",
    ],
  } as ClusterSettings);
};

export const shutdown = async (processOrder: Queue) => {
  for (let i = 0; i < NUM_CPUS; i++) {
    if (cluster.workers[i]) {
      await Delay(100);
      cluster.workers[i].disconnect();
    }
  }

  getLogger(getLoggerID())(
    `Processed ${
      processOrder.getElements().length
    } tables in the following order: ${processOrder.getElements()}`
  );

  rmSync(`${storageFileName}`);

  getLogger(getLoggerID())(`Shutting down master`);
};
