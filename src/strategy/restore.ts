import metadata from "../../metadata.json";
import {generateRestorePath, getIndependentNodes} from "../graph";
import {Restore, Result} from "../types";

export const restore = (): Restore => {
    const {tableDependencies, inDegreeMap} = metadata
    for (const node of Object.keys(tableDependencies)) {
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
        for (const end of Object.keys(tableDependencies)) {
            if (node != end) {
                for (const data of generateRestorePath(node, end, inDegreeMap as any)) {
                    if (!results[end]) {
                        results[end] = [];
                    }
                    data.pop()
                    results[end].push(data)
                }
            }
        }

    }

    return {metadata: metadata as any, results}
}