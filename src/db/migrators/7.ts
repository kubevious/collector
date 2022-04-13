import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ executeSql, sql }) => {

        const queries = [

            sql.createTable('notification_snooze', {
                columns: [
                    { name: 'kind', type: 'VARCHAR(128)', options: 'NOT NULL', isPrimaryKey: true },
                    { name: 'feedback', type: 'BINARY(16)', options: 'NOT NULL', isPrimaryKey: true },
                    { name: 'snooze', type: 'DATETIME', options: 'NULL' },
                ]
            })

        ];

        return Promise.serial(queries, x => executeSql(x));
    })
