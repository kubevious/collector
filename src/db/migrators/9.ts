import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        logger.info("Will be dropping history tables.");

        const queries = [

        sql.dropTable('snapshots'),
        sql.dropTable('snap_items'),
        sql.dropTable('diffs'),
        sql.dropTable('diff_items'),
        sql.dropTable('config_hashes'),
        sql.dropTable('timeline'),

        sql.dropTable('summary_counters'),
        sql.dropTable('summary_counters_by_kind'),
        sql.dropTable('summary_delta_counters'),
        sql.dropTable('summary_delta_counters_by_kind'),

        sql.createTable('snapshots', {
            columns: [
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'snapshot_id', type: 'BINARY(20)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL', isIndexed: true },
                { name: 'prev_snapshot_id', type: 'BINARY(20)', options: 'NULL' },
                { name: 'base_snapshot_id', type: 'BINARY(20)', options: 'NULL' },
                { name: 'summary', type: 'JSON', options: 'NOT NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('snap_items', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'snapshot_id', type: 'BINARY(20)', options: 'NOT NULL', isIndexed: true },
                { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'config_kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'name', type: 'VARCHAR(128)', options: 'NULL DEFAULT \'\'' },
                { name: 'config_hash', type: 'BINARY(32)', options: 'NOT NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('diff_items', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'snapshot_id', type: 'BINARY(20)', options: 'NOT NULL', isIndexed: true },
                { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'config_kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'name', type: 'VARCHAR(128)', options: 'NULL DEFAULT \'\'' },
                { name: 'present', type: 'TINYINT', options: 'NOT NULL' },
                { name: 'config_hash', type: 'BINARY(32)', options: 'NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('delta_items', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'snapshot_id', type: 'BINARY(20)', options: 'NOT NULL', isIndexed: true },
                { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'config_kind', type: 'VARCHAR(128)', options: 'NOT NULL DEFAULT \'\'', isIndexed: true },
                { name: 'name', type: 'VARCHAR(128)', options: 'NULL DEFAULT \'\'' },
                { name: 'present', type: 'TINYINT', options: 'NOT NULL' },
                { name: 'config_hash', type: 'BINARY(32)', options: 'NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('snapshot_configs', {
            columns: [
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'hash', type: 'BINARY(32)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'value', type: 'JSON', options: 'NOT NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('timeline', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'part', type: 'INT UNSIGNED', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'snapshot_id', type: 'BINARY(20)', options: 'NOT NULL', isIndexed: true },
                { name: 'date', type: 'DATETIME', options: 'NOT NULL', isIndexed: true },
                { name: 'changes', type: 'INT', options: 'NOT NULL' },
                { name: 'error', type: 'INT', options: 'NOT NULL' },
                { name: 'warn', type: 'INT', options: 'NOT NULL' },
            ],
            isPartitioned: true
        }),

        sql.createTable('validators', {
            columns: [
                { name: 'validator_id', type: 'VARCHAR(256)', options: 'NOT NULL', isPrimaryKey: true },
                { name: 'setting', type: 'VARCHAR(64)', options: 'NOT NULL' },
            ]
        }),


        ];

        return Promise.serial(queries, x => executeSql(x));
    });
