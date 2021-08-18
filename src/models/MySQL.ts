import { appendFileSync, writeFileSync } from "fs";
import { QueryOptions } from "sequelize";
import { Constraint, StringArrMap, TableConstraints } from "../types";
import { backupFilesDir, promiseExec } from "../strategy";
import { AbstractModel } from "./AbstractModel";

export class MySQL extends AbstractModel {
  async dropTable(table: string, options?: QueryOptions) {
    await this.sequelize.query(
      `
      SET FOREIGN_KEY_CHECKS = 0;
      DROP TABLE IF EXISTS ${this.quoteParamIfNeeded(table)} CASCADE;
      SET FOREIGN_KEY_CHECKS = 1;
      `,
      options
    );
  }

  async generateForeignKeyMap() {
    const query = `
      SELECT COALESCE(JSON_OBJECTAGG(TABLE_NAME, CONSTRAINTS), JSON_OBJECT()) AS data
      FROM (
          SELECT 
              KEY_COLUMN_USAGE.TABLE_NAME,
              JSON_ARRAYAGG(JSON_OBJECT('referencedTable', KEY_COLUMN_USAGE.REFERENCED_TABLE_NAME, 'columnName', COLUMN_NAME, 'referencedColumn', REFERENCED_COLUMN_NAME, 'constraintName', KEY_COLUMN_USAGE.CONSTRAINT_NAME)) AS CONSTRAINTS
          FROM information_schema.KEY_COLUMN_USAGE, information_schema.REFERENTIAL_CONSTRAINTS, information_schema.TABLE_CONSTRAINTS
          WHERE KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA = '${this.config.database}'
            AND KEY_COLUMN_USAGE.CONSTRAINT_NAME = REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME
            AND KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA = REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA
            AND TABLE_CONSTRAINTS.CONSTRAINT_TYPE= 'FOREIGN KEY'
            AND TABLE_CONSTRAINTS.CONSTRAINT_NAME = KEY_COLUMN_USAGE.CONSTRAINT_NAME
          GROUP BY KEY_COLUMN_USAGE.TABLE_NAME
      ) d;
    `;

    return this.execMapQuery(query);
  }

  async executeBackupQuery(table: string) {
    await this.dropTable(table);
    const { stderr } = await promiseExec(
      `MYSQL_PWD="${this.config.password}" mysql --database=${this.config.database} --host="${this.config.host}" --user=${this.config.username} < ${backupFilesDir}/${table}.sql`
    );
    if (stderr) {
      throw stderr;
    }
  }

  async writeTableSchema(table: string) {
    const { stdout: schema, stderr } = await promiseExec(
      await this.getBackupCommand(table.toLowerCase())
    );
    if (stderr) {
      throw stderr;
    }
    writeFileSync(`${backupFilesDir}/${table}.sql`, schema);
  }

  async getBackupCommand(table: string): Promise<string> {
    await this.sequelize.query(`ALTER TABLE ${table} ENGINE=innodb;`);

    return `MYSQL_PWD="${this.config.password}" \
     mysqldump \
     --skip-add-drop-table \
     --skip-comments \
     --no-data \
     --databases ${this.config.database} \
     --host ${this.config.host} \
     --tables ${table} \
     --comments=false \
     --user ${this.config.username}`;
  }

  getDBMetadata(): Promise<StringArrMap> {
    const query = `
    SELECT COALESCE(JSON_OBJECTAGG(table_name, tables), JSON_OBJECT()) AS data
    FROM (SELECT it.table_name,
           COALESCE(k.tables, JSON_ARRAY()) AS tables
        FROM information_schema.tables it
           LEFT JOIN (SELECT kcu.table_name,
                             JSON_ARRAYAGG(kcu.referenced_table_name) AS
                             tables
                      FROM   information_schema.key_column_usage kcu
                      WHERE  kcu.referenced_table_name IS NOT NULL
                      GROUP  BY table_name) k
                  ON k.table_name = it.table_name
        WHERE table_schema = "${this.config.database}") d; 
        `;

    return this.execMapQuery(query);
  }

  getDBInDegreeMap(): Promise<StringArrMap> {
    const query = `
    SELECT COALESCE(JSON_OBJECTAGG(table_name, tables), JSON_OBJECT()) AS data
    FROM   (SELECT COALESCE(JSON_ARRAYAGG(kcu.table_name), JSON_ARRAY()) AS tables,
               it.table_name
        FROM   information_schema.tables it
               LEFT JOIN information_schema.key_column_usage kcu
                      ON kcu.referenced_table_name = it.table_name
        WHERE  it.table_schema = "${this.config.database}"
               AND kcu.referenced_table_name IS NOT NULL
        GROUP  BY it.table_name) d; 
        `;

    return this.execMapQuery(query);
  }

  async formatRowInserts(table: string, querySuffix?: string) {
    const query = `
        SELECT t.* FROM ${table} t
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

    return `INSERT INTO ${table}(${columns}) VALUES(${values});\n`;
  }

  generateAlterTableCommand(table: string, constraint: Constraint) {
    return `ALTER TABLE ${table} ADD CONSTRAINT ${constraint.constraintName} FOREIGN KEY (${constraint.columnName}) REFERENCES ${constraint.referencedTable} (${constraint.referencedColumn}) ON DELETE CASCADE ON UPDATE CASCADE;\n`;
  }

  quoteParamIfNeeded(param: string) {
    return param;
  }

  writeContraintQueries(table: string, constraintMap: TableConstraints) {
    if (constraintMap[table]) {
      for (const constraint of constraintMap[table]) {
        appendFileSync(
          this.getBackupFilePath(table),
          this.generateAlterTableCommand(table, constraint)
        );
      }
    }
  }
}
