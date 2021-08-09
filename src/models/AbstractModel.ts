import { Options, QueryTypes, Sequelize } from "sequelize";
import { QueryData, StringArrMap } from "../types";

export abstract class AbstractModel {
  sequelize: Sequelize;

  constructor(config: Options) {
    this.sequelize = new Sequelize(config);

    return this;
  }

  async execMapQuery(query, bind = []): Promise<StringArrMap> {
    const { data } = await this.sequelize.query<QueryData>(query, {
      type: QueryTypes.SELECT,
      benchmark: true,
      plain: true,
      bind,
    });

    return data;
  }

  abstract getBackupCommand(config: Options, table: string): string;

  abstract getPostgresInDegreeMap(): Promise<StringArrMap>;

  abstract getPostgresDBMetadata(): Promise<StringArrMap>;
}
