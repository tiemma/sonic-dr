import { appendFileSync } from "fs";
import { Options, QueryOptions, QueryTypes, Sequelize } from "sequelize";
import { StringArrMap, TableConstraints } from "../types";
import { promiseExec, readBackup } from "../strategy/utils";
import { AbstractModel } from "./AbstractModel";

(<any>Sequelize).postgres.DECIMAL.parse = parseFloat;
require("pg").defaults.parseInt8 = true;

export class Postgres extends AbstractModel {
  async dropTable(table: string, options?: QueryOptions) {
    await this.sequelize.query(
      `DROP TABLE IF EXISTS ${this.quoteParamIfNeeded(table)} CASCADE;`,
      options
    );
  }

  async executeBackupQuery(table: string) {
    await this.sequelize.transaction(async (transaction) => {
      const queryOptions: QueryOptions = {
        transaction,
        logging: true,
        benchmark: true,
        raw: true,
      };
      await this.dropTable(table, queryOptions);
      await this.sequelize.query(readBackup(table), {
        type: QueryTypes.INSERT,
        ...queryOptions,
      });
    });
  }

  async writeTableSchema(table: string) {
    await promiseExec(this.getBackupCommand(table.toLowerCase()));
  }

  getBackupCommand(table: string): string {
    return ` 
      PGPASSWORD=${this.config.password} \
      pg_dump \
          -h ${this.config.host} \
          -d ${this.config.database}  \
          -U ${this.config.username} \
          -f ${this.getBackupFilePath(table)} \
          -t "${table}" \
          -O \
          -x \
          -s \
          --format=plain \
          --no-tablespaces \
          --no-publications \
          --no-security-labels \
          --no-subscriptions \
          --no-synchronized-snapshots \
          --no-unlogged-table-data \
          --quote-all-identifiers \
          --no-comments \
          --disable-triggers
      `;
  }

  getDBInDegreeMap(): Promise<StringArrMap> {
    const indegreeQuery = `
            SELECT JSONB_OBJECT_AGG(name, "foreignKeyTables") || '{}'::JSONB AS data
            FROM (
                SELECT
                    ccu.table_name AS name,
                    ARRAY_TO_JSON(COALESCE(ARRAY_AGG(DISTINCT t.table_name), ARRAY[]::VARCHAR[])) AS "foreignKeyTables"
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

  async getDBMetadata(): Promise<StringArrMap> {
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

  async formatRowInserts(table: string, querySuffix?: string) {
    const query = `
        SELECT t.* FROM "${table}" t
        ${querySuffix || ""}
    `;
    const data = await this.sequelize.query(query, this.queryOptions());
    for (const row of Object.values(data as any[])) {
      appendFileSync(
        this.getBackupFilePath(table),
        this.generateInsertCommand(table, row)
      );
    }
  }

  generateInsertCommand(table: string, row: Record<string, any>): string {
    const { columns, values } = this.cleanRowData(row);

    return `INSERT INTO "public"."${table}"(${columns})  VALUES(${values});\n`;
  }

  quoteParamIfNeeded(param: string) {
    return `"${param}"`;
  }

  generateForeignKeyMap(): Promise<TableConstraints> {
    return Promise.resolve(undefined);
  }

  writeContraintQueries(_table: string, _constraints: TableConstraints) {}
}
