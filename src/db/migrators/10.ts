import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        const queries = [

        sql.createTable('logic_item_data', {
            columns: [
                { name: 'id', type: 'INT UNSIGNED', options: 'NOT NULL AUTO_INCREMENT', isPrimaryKey: true },
                { name: 'dn', type: 'VARCHAR(1024)', options: 'NOT NULL' },
                { name: 'key', type: 'VARCHAR(128)', options: 'NOT NULL' },
                { name: 'value', type: 'JSON', options: 'NOT NULL' },
            ]
        }),

        ];

        return Promise.serial(queries, x => executeSql(x));
    });
