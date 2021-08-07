export type StringArrMap = Record<string, Set<string>>;

export type QueryData = Record<"data", StringArrMap>;

export type Result = Record<string, string[][]>

export interface DBMetadataGraph {
    tableDependencies: StringArrMap;
    inDegreeMap: StringArrMap
}

export interface MapReduceEvent {
    table: string;
    // SYN defined on worker start
    SYN?: boolean
    ACK?: boolean;
    SYN_ACK?: boolean
}

export interface Restore {
    results: Result
    metadata: DBMetadataGraph
}