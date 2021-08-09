import metadata from "../../metadata.json";
import {generateRestorePath, getIndependentNodes} from "../graph";
import {Restore, Result} from "../types";

export const restore = (): Restore => {
    const {tableDependencies, inDegreeMap} = metadata
    const dependencies = Object.keys(tableDependencies)
    for (const node of dependencies) {
        tableDependencies[node] = new Set(tableDependencies[node])
        if (inDegreeMap[node]) {
            inDegreeMap[node] = new Set(inDegreeMap[node])
        }
    }

    const queue = getIndependentNodes(tableDependencies as any)
    if (queue.isEmpty()) {
        throw "Cyclic dependency list"
    }

    const results: Result = {}
    while (!queue.isEmpty()) {
        const node = queue.dequeue()
        if (!results[node]) {
            results[node] = [];
        }
        for (const end of dependencies) {
            if (!results[end]) {
                results[end] = [];
            }
            if (node != end) {
                for (const data of generateRestorePath(node, end, inDegreeMap as any)) {
                    results[end].push(data)
                }
            }
        }

    }

    return {metadata: metadata as any, results}
}