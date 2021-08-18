import { Options, QueryOptions, QueryTypes, Sequelize } from "sequelize";
import { QueryData, StringArrMap, TableConstraints } from "../types";
import { backupFilesDir } from "../strategy";

export abstract class AbstractModel {
  sequelize: Sequelize;
  config: Options;

  constructor(config: Options) {
    this.config = {
      ...config,
      logging: false,
      benchmark: false,
      dialectOptions: {
        multipleStatements: true,
      },
    };
    this.sequelize = new Sequelize(this.config);

    return this;
  }

  async execMapQuery(query, bind = []) {
    const { data } = await this.sequelize.query<QueryData>(
      query,
      this.queryOptions({ bind, plain: true }) as any
    );

    return data as any;
  }

  queryOptions(args?: Record<string, any>): QueryOptions {
    return {
      type: QueryTypes.SELECT,
      benchmark: true,
      plain: args?.plain,
      bind: args?.bind,
    };
  }

  getBackupFilePath(table: string): string {
    return `${backupFilesDir}/${table}.sql`;
  }

  cleanRowData(row: any) {
    const columns = Object.keys(row)
      .map((x) => this.quoteParamIfNeeded(x))
      .join(",");
    const values = Object.values(row)
      .map((x) => `'${JSON.stringify(x).replace(/^"(.+)"$/, "$1")}'`)
      .join(",")
      .replace(/'null'/g, "NULL");

    return { columns, values };
  }

  abstract getBackupCommand(table: string);

  abstract getDBInDegreeMap(): Promise<StringArrMap>;

  abstract getDBMetadata(): Promise<StringArrMap>;

  abstract formatRowInserts(table: string, querySuffix: string);

  abstract quoteParamIfNeeded(param: string): string;

  abstract writeTableSchema(table: string);

  abstract executeBackupQuery(table: string);

  abstract generateForeignKeyMap(): Promise<TableConstraints>;

  abstract writeContraintQueries(
    table: string,
    constraintMap: TableConstraints
  );

  abstract dropTable(table: string, options?: QueryOptions);
}
