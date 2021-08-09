import {
  DependencyGraph,
  topologicalDependencySort,
  Queue,
} from "@tiemma/sonic-core";
import { StringArrMap } from "../types";

export const convertIntoGraphAndSort = (metadata: StringArrMap) => {
  const dependencyGraph: DependencyGraph = {};
  for (const [name, foreignKeys] of Object.entries(metadata)) {
    dependencyGraph[name] = {
      dependencies: Array.from(foreignKeys),
    };
  }

  return topologicalDependencySort(dependencyGraph);
};

export const getIndependentNodes = (metadata: StringArrMap) => {
  const independentNodes = new Queue();

  for (const [node, dependencies] of Object.entries(metadata)) {
    if (dependencies.size === 0) {
      independentNodes.enqueue(node);
    }
  }

  return independentNodes;
};

export const generateRestorePath = (
  start: string,
  end: string,
  graph: StringArrMap
) => {
  const isVisited: { [key: string]: boolean } = {};
  const result = [];

  const getAllPaths = (currNode: string, pathList: string[]) => {
    if (currNode === end) {
      const paths = Array.from(pathList);
      paths.pop();
      result.push(paths);
    }
    if (graph[currNode]) {
      for (const node of graph[currNode]) {
        if (!isVisited[node]) {
          pathList.push(node);
          getAllPaths(node, pathList);

          pathList.splice(pathList.indexOf(node), 1);
        }
      }
    }

    isVisited[currNode] = false;
  };

  getAllPaths(start, [start]);

  return result;
};
