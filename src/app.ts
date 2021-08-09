import { Options } from "sequelize";
import config from "../config.json";
import { backup, restore } from "./strategy";

(async () => {
  await backup(config as Options);
  await restore();
})();
