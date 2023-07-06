import _ from 'the-lodash';

import { Migrator } from '../migration';
import { MyPromise } from 'the-promise';

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
        
        return MyPromise.serial(queries, x => executeSql(x));

    });