import _ from 'the-lodash';

import { Migrator } from '../migration';
import { MyPromise } from 'the-promise';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        const queries = [

            "ALTER TABLE logic_item_data ADD latest_part INT UNSIGNED DEFAULT 0;"

        ];

        return MyPromise.serial(queries, x => executeSql(x));
    });
