import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { promisify } from "util";
import { exec } from "child_process";
import { DataTypes, Sequelize } from "sequelize";
import { getWorkerName } from "@tiemma/sonic-distribute";
import { DBMetadataGraph } from "../types";
import { getLogger } from "../utils";

export const backupDir = `${process.cwd()}/backup`;
export const backupFilesDir = `${backupDir}/files`;
const logger = getLogger(getWorkerName());

export const createDirIfMissing = (dir: string) => {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
};

export enum MetadataFiles {
  METADATA_GRAPH = "metadata.json",
  ADJACENCY_MATRIX = "adjMatrix.json",
}

export const backupMetadata = (
  dir: string,
  fileName: MetadataFiles,
  data: any
) => {
  createDirIfMissing(backupFilesDir);
  writeFileSync(`${dir}/${fileName}`, JSON.stringify(data, null, "\t"));
};

export const getCount = (metadata: DBMetadataGraph, table: string) => {
  return Tables.sequelize.query(
    `SELECT COUNT(*) AS count FROM tables WHERE name IN (:names) AND isProcessed IS TRUE`,
    {
      replacements: {
        names: Array.from(metadata.tableDependencies[table]),
      },
      plain: true,
      logging: false,
    }
  );
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

export const ensureDependenciesSatisfied = async (
  metadata: DBMetadataGraph,
  table: string
) => {
  const dependencySize = (metadata.tableDependencies[table] as string[]).length;
  if (dependencySize === 0) {
    return;
  }

  let filledTables = await getCount(metadata, table);
  while (filledTables["count"] !== dependencySize) {
    filledTables = await getCount(metadata, table);
  }
  logger(
    `Dependencies fulfilled for table ${table}, expected=${dependencySize}, found=${filledTables["count"]}`
  );
};

export const isProcessed = async (table: string) => {
  const count = await Tables.count({ where: { name: table } });

  if (!count) {
    logger(`Unprocessed ${table} found`);
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

export const readBackup = (table: string) =>
  readFileSync(`${backupFilesDir}/${table}.sql`, {
    encoding: "utf-8",
  }).toString();

export const promiseExec = promisify(exec);
