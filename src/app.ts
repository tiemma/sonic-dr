import { isMaster, sonicDistribute } from "@tiemma/sonic-distribute";
import { strategyMap } from "./strategy";
import { DRArgs } from "./types";

export const sonicDR = async (op: "backup" | "restore", args: DRArgs) => {
  const { masterFn, workerFns, reduceFn } = strategyMap[op];
  const data = await sonicDistribute(masterFn, workerFns, reduceFn, args);

  if (isMaster()) {
    return data;
  }
};
