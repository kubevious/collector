import { Context } from "../context";
import { ILogger } from "the-logger";
import { Promise } from 'the-promise';

import { DataStore, MySqlDriver, MySqlStatement } from '@kubevious/easy-data-store';

export interface MigratorArgs
{
    logger: ILogger;
    driver: MySqlDriver;
    executeSql: (sql: string, params?: any[]) => Promise<any>;
    context: Context;
    sql: SqlBuilder;
}

export interface MigratorInfo
{
    handler?: (args: MigratorArgs) => Promise<any>;
}

export function Migrator() : MigratorBuilder
{
    return new MigratorBuilder();
}

export class MigratorBuilder
{
    private _data: MigratorInfo = {};

    handler(value: (args: MigratorArgs) => Promise<any>)
    {
        this._data.handler = value;
        return this;
    }

    _export() : MigratorInfo
    {
        if (!this._data.handler) {
            throw new Error("Handler not set.");
        }
        return this._data;
    }
}

export class SqlBuilder
{
    dropTable(name: string)
    {
        return `DROP TABLE IF EXISTS \`${name}\``;
    }

    createTable(name: string, params: SqlBuilderCreateTableParams)
    {
        const columnsStr : string[] = [];

        let sql = `CREATE TABLE IF NOT EXISTS \`${name}\` ( `;

        for(const column of params.columns)
        {
            columnsStr.push(`\`${column.name}\` ${column.type} ${column.options}`);
        }

        {
            const pks = params.columns.filter(x => x.isPrimaryKey).map(x => x.name);
            const pksStr = pks.map(x => `\`${x}\``);
            columnsStr.push(`PRIMARY KEY (${pksStr})`);
        }

        {
            for(const column of params.columns)
            {
                if (column.isIndexed && !column.isPrimaryKey)
                {
                    columnsStr.push(`KEY \`${column.name}\` (\`${column.name}\`)`);
                }
            }
        }

        sql += columnsStr.join(', ');

        sql += `) ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci `;

        if (params.isPartitioned)
        {
            sql += 
                "PARTITION BY RANGE (`part`) ( " +
                    "PARTITION p0 VALUES LESS THAN (0) " +
                ")";
        }

        sql += ';';
        return sql;
    }
}

export interface SqlBuilderCreateTableParams
{
    columns: SqlBuilderCreateTableColumn[];
    isPartitioned?: boolean;
}

export interface SqlBuilderCreateTableColumn
{
    name: string;
    type: string;
    options?: string;
    isPrimaryKey?: boolean
    isIndexed?: boolean
}