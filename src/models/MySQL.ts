import { appendFileSync, writeFileSync } from "fs";
import { StringArrMap } from "../types";
import { backupFilesDir, promiseExec } from "../strategy";
import { AbstractModel } from "./AbstractModel";

export class MySQL extends AbstractModel {
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
     --add-drop-table \
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

  quoteParamIfNeeded(param: string) {
    return param;
  }
}
