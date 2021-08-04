import {Options, Sequelize} from "sequelize";
import config from "../config.json"

export const sequelize = new Sequelize(config as Options)