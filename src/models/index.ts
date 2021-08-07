import {Postgres} from "./Postgres";
import {Options} from "sequelize";

export const modelMap = {
    "postgres": Postgres
}

export const getDBInstance = (config: Options) => {
    return new modelMap[config.dialect](config)
}