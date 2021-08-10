import { Options, QueryOptions } from "sequelize";
import { StringArrMap } from "../types";
import { backupFilesDir } from "../strategy/utils";
import { AbstractModel } from "./AbstractModel";

export class MySQL extends AbstractModel {
  async dropTable(table: string, options?: QueryOptions) {
    await this.sequelize.query(
      `
            SET FOREIGN_KEY_CHECKS = 0;
            DROP TABLE IF EXISTS ${table};
            SET FOREIGN_KEY_CHECKS = 1;
        `,
      options
    );
  }

  getBackupCommand(config: Options, table: string): string {
    return `mysqldump -y  --add-drop-table -c -h ${config.host} --tables ${table} -U ${config.username} -p ${config.password} > ${backupFilesDir}/${table}.sql`;
  }

  getDBMetadata(): Promise<StringArrMap> {
    const query = `
    SELECT COALESCE(Json_objectagg(table_name, tables), Json_object()) AS data
    FROM (SELECT it.table_name,
           COALESCE(k.tables, Json_array()) AS tables
        FROM   information_schema.tables it
           LEFT JOIN (SELECT kcu.table_name,
                             Json_arrayagg(kcu.referenced_table_name) AS
                             tables
                      FROM   information_schema.key_column_usage kcu
                      WHERE  kcu.referenced_table_name IS NOT NULL
                      GROUP  BY table_name) k
                  ON k.table_name = it.table_name
        WHERE  table_schema = "test") d; 
        `;

    return this.execMapQuery(query);
  }

  getDBInDegreeMap(): Promise<StringArrMap> {
    const query = `
    SELECT COALESCE(Json_objectagg(table_name, tables), Json_object()) AS data
    FROM   (SELECT COALESCE(Json_arrayagg(kcu.table_name), Json_array()) AS tables,
               it.table_name
        FROM   information_schema.tables it
               LEFT JOIN information_schema.key_column_usage kcu
                      ON kcu.referenced_table_name = it.table_name
        WHERE  it.table_schema = "test"
               AND kcu.referenced_table_name IS NOT NULL
        GROUP  BY it.table_name) d; 
        `;

    return this.execMapQuery(query);
  }
}
