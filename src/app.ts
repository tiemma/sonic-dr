import { isMaster, MapReduce } from "@tiemma/sonic-distribute";
import config from "../config.json";
import { strategyMap } from "./strategy";

(async () => {
  const { masterFn, workerFns, reduceFn } = strategyMap.restore;
  const data = await MapReduce(masterFn, workerFns, reduceFn, {
    config,
    numWorkers: 2,
    tableSuffixes: {
      cluster: "WHERE name LIKE 'c%'",
    },
  });

  if (isMaster()) {
    console.log(data);
    process.exit(0);
  }
})();
