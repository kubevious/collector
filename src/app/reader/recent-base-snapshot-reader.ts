import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Context } from '../../context'

import { DBSnapshotProcessableData } from './types'

import * as BufferUtils from '@kubevious/helpers/dist/buffer-utils';
import { SnapshotReader } from './snapshot-reader';
import { LatestSnapshotIdConfig, LATEST_SNAPSHOT_CONFIG_NAME } from '../persistor/types';

export class RecentBaseSnapshotReader
{
    private _logger : ILogger;
    private _context : Context;

    constructor(logger: ILogger, context : Context)
    {
        this._context = context;
        this._logger = logger.sublogger('RecentSnapshotReader');
    }

    query() : Promise<DBSnapshotProcessableData | null>
    {
        return this._queryLatestSnapshot()
            .then(latestSnapshotConfig => {
                this._logger.warn("LATEST_CONFIG: ", latestSnapshotConfig);


                if (!latestSnapshotConfig) {
                    this._logger.warn("No Latest Snapshot")
                    return null;
                }
                if (!latestSnapshotConfig.snapshot_id) {
                    this._logger.warn("No Latest Snapshot ID");
                    return null;
                }

                this._logger.info("Latest Snapshot: %s", latestSnapshotConfig.snapshot_id)

                const reader = new SnapshotReader(this._logger, this._context, {
                    snapshotId: BufferUtils.fromStr(latestSnapshotConfig.snapshot_id)
                })

                return reader.queryProcessableData();
            })
    
    }

    private _queryLatestSnapshot()
    {
        return this._context.dataStore.getConfig<LatestSnapshotIdConfig | null>(LATEST_SNAPSHOT_CONFIG_NAME, null);
    }

}
