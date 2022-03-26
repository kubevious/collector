import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Context } from '../../context'

import { DBSnapshotProcessableData } from './types'

import { BufferUtils } from '@kubevious/data-models';
import { SnapshotReader } from './snapshot-reader';

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
            .then(latestSnapshotId => {

                if (!latestSnapshotId) {
                    this._logger.warn("No Latest Snapshot")
                    return null;
                }

                this._logger.info("Latest Snapshot: %s", latestSnapshotId)

                const reader = new SnapshotReader(this._logger, this._context, {
                    snapshotId: BufferUtils.fromStr(latestSnapshotId)
                })

                return reader.queryProcessableData();
            })
    
    }

    private _queryLatestSnapshot()
    {
        return this._context.configAccessor.getLatestSnapshotId();
    }

}
