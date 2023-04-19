import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Migrator } from '../migration';

export default Migrator()
    .handler(({ logger, executeSql, sql }) => {
        
        const queries = [

            "ALTER TABLE logic_item_data ADD latest_part INT UNSIGNED DEFAULT 0;"

        ];

        return Promise.serial(queries, x => executeSql(x));
    });
