import metadata from "../../backup/metadata.json";
import { generateRestorePath, getIndependentNodes } from "../graph";
import { Restore, Result } from "../types";

export const restore = (): Restore => {
  const { tableDependencies, inDegreeMap } = metadata as any;
  const dependencies = Object.keys(tableDependencies);

  for (const node of dependencies) {
    tableDependencies[node] = new Set(tableDependencies[node]);
    if (inDegreeMap[node]) {
      inDegreeMap[node] = new Set(inDegreeMap[node]);
    }
  }

  const queue = getIndependentNodes(tableDependencies);
  if (queue.isEmpty()) {
    throw "Cyclic dependency list";
  }

  const results: Result = {};
  while (!queue.isEmpty()) {
    const node = queue.dequeue();
    if (!results[node]) {
      results[node] = [];
    }

    for (const end of dependencies) {
      if (node === end) continue;

      if (!results[end]) {
        results[end] = [];
      }

      for (const data of generateRestorePath(node, end, inDegreeMap)) {
        results[end].push(data);
      }
    }
  }

  return { metadata: metadata as any, results };
};

restore();
