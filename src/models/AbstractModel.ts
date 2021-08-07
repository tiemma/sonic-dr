import {Options, Sequelize} from "sequelize";
import {StringArrMap} from "../types";

export abstract class AbstractModel {
    sequelize: Sequelize;

    constructor(config: Options) {
        this.sequelize = new Sequelize(config)

        return this;
    }

    abstract getBackupCommand(config: Options, table: string): string

    abstract execMapQuery(query, bind: string[]): Promise<StringArrMap>

    abstract getPostgresInDegreeMap(): Promise<StringArrMap>

    abstract getPostgresDBMetadata(): Promise<StringArrMap>
}