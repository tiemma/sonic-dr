export interface Table {
    name: string;
    foreignKeys: string[];
}

export interface DependencyGraph {
    topologicallySortedTableNames: string[];
    tables: Table[];
}