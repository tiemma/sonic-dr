import { Options } from "sequelize";
import { Postgres } from "./Postgres";

export const modelMap = {
  postgres: Postgres,
};

export const getDBInstance = (config: Options) => {
  return new modelMap[config.dialect](config);
};
