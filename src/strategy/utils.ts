import { existsSync, mkdirSync, writeFileSync } from "fs";
import { DBMetadataGraph } from "../types";

export const backupDir = `${process.cwd()}/backup`;
export const backupFilesDir = `${backupDir}/files`;

export const createDirIfMissing = (dir: string) => {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
};

export const backupMetadata = (dir: string, data: DBMetadataGraph) => {
  createDirIfMissing(backupFilesDir);
  writeFileSync(`${dir}/metadata.json`, JSON.stringify(data, null, "\t"));
};
