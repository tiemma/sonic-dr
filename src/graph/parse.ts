import { StringArrMap} from "../types";
import {DependencyGraph, topologicalDependencySort} from "@tiemma/sonic-core";

export const convertIntoGraphAndSort = (metadata: StringArrMap) => {
    const dependencyGraph: DependencyGraph = {}
    for(const[name, foreignKeys] of Object.entries(metadata)) {
        dependencyGraph[name] = {
            dependencies: foreignKeys
        }
    }

    return topologicalDependencySort(dependencyGraph);
}