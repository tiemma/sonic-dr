import {backup, restore} from "./strategy";
import config from "../config.json"
import {Options} from "sequelize";

(async () =>{
    await backup(config as Options)
    await restore()
} )();
