import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ executeSql, sql }) => {
        
        const queries = [

            sql.createTable('config', {
                columns: [
                    { name: 'key', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                    { name: 'value', type: 'JSON', options: 'NOT NULL' },
                ]
            })

        ];
        
        return Promise.serial(queries, x => executeSql(x));

    });