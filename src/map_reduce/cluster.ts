import cluster from "cluster";
import { initMaster, initWorkers, isMaster, shutdown } from "./utils";

export const MapReduce = async (
  masterFn: any,
  workerFn: any,
  ReduceFn: any,
  args: any
) => {
  if (isMaster()) {
    const { workerQueue, processOrder } = await initMaster(args);

    await masterFn(args, workerQueue);

    await shutdown();

    return ReduceFn(processOrder);
  } else if (cluster.isWorker) {
    await initWorkers(workerFn, args);
  }
};
