import _ from 'the-lodash';

import { Migrator } from '../migration';
import { MyPromise } from 'the-promise';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        const queries = [

        sql.createTable('guard_change_packages', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'change_id', type: 'VARCHAR(800)', options: 'NOT NULL', isIndexed: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL', isIndexed: true },
                { name: 'source', type: 'JSON', options: 'NOT NULL' },
                { name: 'summary', type: 'JSON', options: 'NOT NULL' },
                { name: 'charts', type: 'JSON', options: 'NOT NULL' },
                { name: 'changes', type: 'JSON', options: 'NOT NULL' },
                { name: 'deletions', type: 'JSON', options: 'NOT NULL' },
            ]
        }),

        sql.createTable('guard_validation_queue', {
            columns: [
                { name: 'change_id', type: 'VARCHAR(800)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
            ]
        }),

        sql.createTable('guard_validation_history', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'change_id', type: 'VARCHAR(800)', options: 'NOT NULL', isIndexed: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                { name: 'state', type: 'VARCHAR(128)', options: 'NOT NULL' },
            ]
        }),

        sql.createTable('guard_validation_states', {
            columns: [
                { name: 'change_id', type: 'VARCHAR(800)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                { name: 'state', type: 'VARCHAR(128)', options: 'NOT NULL' },
                { name: 'success', type: 'TINYINT', options: 'NULL' },
                { name: 'summary', type: 'JSON', options: 'NULL' },
                { name: 'newIssues', type: 'JSON', options: 'NULL' },
                { name: 'clearedIssues', type: 'JSON', options: 'NULL' },
            ]
        }),

        ];

        return MyPromise.serial(queries, x => executeSql(x));
    });
