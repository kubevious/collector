import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        const queries = [

        sql.createTable('guard_change_packages', {
            columns: [
                { name: 'namespace', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'name', type: 'VARCHAR(512)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                { name: 'summary', type: 'JSON', options: 'NOT NULL' },
                { name: 'charts', type: 'JSON', options: 'NOT NULL' },
                { name: 'changes', type: 'JSON', options: 'NOT NULL' },
                { name: 'deletions', type: 'JSON', options: 'NOT NULL' },
            ]
        }),

        sql.createTable('guard_validation_queue', {
            columns: [
                { name: 'namespace', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'name', type: 'VARCHAR(512)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
            ]
        }),

        sql.createTable('guard_validation_history', {
            columns: [
                { name: 'namespace', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'name', type: 'VARCHAR(512)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'state', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
            ]
        }),

        sql.createTable('guard_validation_states', {
            columns: [
                { name: 'namespace', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'name', type: 'VARCHAR(512)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL' },
                { name: 'state', type: 'VARCHAR(128)', options: 'NOT NULL' },
                { name: 'success', type: 'TINYINT', options: 'NULL' },
                { name: 'summary', type: 'JSON', options: 'NULL' },
                { name: 'newIssues', type: 'JSON', options: 'NULL' },
                { name: 'clearedIssues', type: 'JSON', options: 'NULL' },
            ]
        }),

        ];

        return Promise.serial(queries, x => executeSql(x));
    });
