import { backupOpsMap } from "./backup";
import { restoreOpsMap } from "./restore";

export * from "./backup";
export * from "./restore";
export * from "./utils";

export const strategyMap = {
  backup: backupOpsMap,
  restore: restoreOpsMap,
};
