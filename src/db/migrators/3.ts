import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ executeSql, sql }) => {
        
        const queries = [

            sql.createTable('rules', {
                columns: [
                    { name: 'name', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                    { name: 'enabled', type: 'TINYINT', options: 'NOT NULL' },
                    { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                    { name: 'target', type: 'TEXT', options: 'NOT NULL' },
                    { name: 'script', type: 'TEXT', options: 'NOT NULL' },
                    { name: 'hash', type: 'BINARY(32)', options: 'NULL' },
                ]
            }),

            sql.createTable('rule_statuses', {
                columns: [
                    { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                    { name: 'rule_name', type: 'VARCHAR(128)', options: 'NOT NULL', isIndexed: true},
                    { name: 'hash', type: 'BINARY(32)', options: 'NOT NULL' },
                    { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                    { name: 'error_count', type: 'INT UNSIGNED', options: 'NOT NULL' },
                    { name: 'item_count', type: 'INT UNSIGNED', options: 'NOT NULL' },
                ]
            }),

            sql.createTable('rule_logs', {
                columns: [
                    { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                    { name: 'rule_name', type: 'VARCHAR(128)', options: 'NOT NULL', isIndexed: true},
                    { name: 'kind', type: 'VARCHAR(128)', options: 'NOT NULL' },
                    { name: 'msg', type: 'JSON', options: 'NOT NULL' },
                ]
            }),

            sql.createTable('rule_items', {
                columns: [
                    { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                    { name: 'rule_name', type: 'VARCHAR(128)', options: 'NOT NULL', isIndexed: true},
                    { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL' },
                    { name: 'errors', type: 'INT UNSIGNED', options: 'NOT NULL' },
                    { name: 'warnings', type: 'INT UNSIGNED', options: 'NOT NULL' },
                    { name: 'markers', type: 'JSON', options: 'NULL' },
                ]
            }),

            sql.createTable('markers', {
                columns: [
                    { name: 'name', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                    { name: 'shape', type: 'VARCHAR(128)', options: 'NOT NULL' },
                    { name: 'color', type: 'VARCHAR(128)', options: 'NOT NULL' },
                    { name: 'propagate', type: 'TINYINT', options: 'NOT NULL' },
                ]
            }),


            sql.createTable('marker_items', {
                columns: [
                    { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                    { name: 'marker_name', type: 'VARCHAR(128)', options: 'NOT NULL', isIndexed: true},
                    { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL' },
                ]
            }),

        ];

        return Promise.serial(queries, x => executeSql(x));
    });