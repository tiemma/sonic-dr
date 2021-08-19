import { Options } from "sequelize";

export type StringArrMap = Record<string, string[] | Set<string>>;

export type QueryData = Record<"data", StringArrMap | TableConstraints>;

export type Result = Record<string, string[][]>;

export interface DBMetadataGraph {
  tableDependencies: StringArrMap;
  inDegreeMap: StringArrMap;
}

export interface Restore {
  results: Result;
  metadata: DBMetadataGraph;
}

export interface Constraint {
  columnName: string;
  constraintName: string;
  referencedTable: string;
  referencedColumn: string;
}

export type TableConstraints = Record<string, Constraint[]>;

export interface DRArgs {
  numWorkers: number;
  config: Options;
  tableSuffixes: Record<string, string>;
}
