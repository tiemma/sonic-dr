import {QueryData, StringArrMap} from "../types";
import {Options, QueryTypes, Sequelize} from "sequelize";

(<any>Sequelize).postgres.DECIMAL.parse = parseFloat;
require("pg").defaults.parseInt8 = true;

export class Postgres {
    static sequelize: Sequelize;

    static init(config: Options){
        this.sequelize =  new Sequelize(config)

        return this;
    }

    static async execMapQuery(query, bind = []): Promise<StringArrMap> {
        const {data} = await this.sequelize.query<QueryData>(query, {
            type: QueryTypes.SELECT,
            benchmark: true,
            plain: true,
            bind
        });

        return data
    }

    static async getPostgresDBMetadata(): Promise<StringArrMap> {
        const postgresTableRelationshipQuery = `
            SELECT JSONB_OBJECT_AGG(name, "foreignKeyTables") || '{}'::JSONB AS data
            FROM (
                SELECT 
                    t.table_name AS name, 
                    ARRAY_TO_JSON(COALESCE(ARRAY_AGG(DISTINCT ccu.table_name) FILTER (WHERE ccu.table_name != tc.table_name), ARRAY[]::VARCHAR[])) AS "foreignKeyTables"
                FROM information_schema.tables t
                    LEFT JOIN information_schema.table_constraints tc ON tc.table_name = t.table_name 
                    LEFT JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema='public'
                GROUP BY t.table_name
            ) d;
        `;

        return this.execMapQuery(postgresTableRelationshipQuery)
    }
}