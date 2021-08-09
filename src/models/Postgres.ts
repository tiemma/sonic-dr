import { Options, QueryTypes, Sequelize } from "sequelize";
import { QueryData, StringArrMap } from "../types";
import { backupFilesDir } from "../strategy/utils";
import { AbstractModel } from "./AbstractModel";

(<any>Sequelize).postgres.DECIMAL.parse = parseFloat;
require("pg").defaults.parseInt8 = true;

export class Postgres extends AbstractModel {
  getBackupCommand(config: Options, table: string): string {
    return ` 
            PGPASSWORD=${config.password} \
            pg_dump \
                -h ${config.host} \
                -d ${config.database}  \
                -U ${config.username} \
                -f "${backupFilesDir}/${table}.sql" \
                -t "${table}" \
                -O \
                -x  \
                -c \
                --column-inserts \
                --format=plain \
                --inserts \
                --if-exists \
                --no-tablespaces \
                --no-publications \
                --no-security-labels \
                --no-subscriptions \
                --no-synchronized-snapshots \
                --no-unlogged-table-data \
                --quote-all-identifiers 
          `;
  }

  getPostgresInDegreeMap(): Promise<StringArrMap> {
    const indegreeQuery = `
            SELECT JSONB_OBJECT_AGG(name, "foreignKeyTables") || '{}'::JSONB AS data
            FROM (
                SELECT
                    ccu.table_name AS name,
                    ARRAY_TO_JSON(COALESCE(ARRAY_AGG(DISTINCT t.table_name) FILTER (WHERE ccu.table_name != tc.table_name), ARRAY[]::VARCHAR[])) AS "foreignKeyTables"
                FROM information_schema.tables t
                    LEFT JOIN information_schema.table_constraints tc ON tc.table_name = t.table_name
                    LEFT JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema='public' AND ccu.table_name != t.table_name
                GROUP BY ccu.table_name
            ) d;
        `;

    return this.execMapQuery(indegreeQuery);
  }

  async getPostgresDBMetadata(): Promise<StringArrMap> {
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

    return this.execMapQuery(postgresTableRelationshipQuery);
  }
}
