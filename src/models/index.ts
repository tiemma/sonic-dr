import { Options } from "sequelize";
import { Postgres } from "./Postgres";
import { MySQL } from "./MySQL";
import { AbstractModel } from "./AbstractModel";

export const modelMap = {
  postgres: Postgres,
  mysql: MySQL,
};

export const getDBInstance = (config: Options): AbstractModel => {
  return new modelMap[config.dialect](config);
};
