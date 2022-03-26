import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { Context } from '../../context'

import { ITableAccessor } from '@kubevious/easy-data-store';

import { DBSnapshotProcessableData, SnapshotReaderTarget } from './types'
import { DBSnapshot } from './snapshot'

import { BufferUtils } from '@kubevious/data-models';
import { UuidUtils } from '@kubevious/data-models';

import { DeltaSummary } from '../summary/types';

export class SnapshotReader
{
    private _logger : ILogger;
    private _context : Context;
    private _target: SnapshotReaderTarget;

    private _dataStore : ITableAccessor;

    private _partId : number;
    private _snapshotId: Buffer;

    constructor(logger: ILogger, context : Context, target: SnapshotReaderTarget)
    {
        this._context = context;
        this._logger = logger.sublogger('SnapshotReader');

        this._target = target;

        this._snapshotId = target.snapshotId;

        this._dataStore = context.dataStore.dataStore;

        this._partId = UuidUtils.getPartFromDatedUUIDBuf(target.snapshotId);
    }

    queryProcessableData() : Promise<DBSnapshotProcessableData | null>
    {
        return this.querySnapshotRow()
            .then(snapshotRow => {
                if (!snapshotRow) {
                    return null;
                }

                let snapItemsOwnerId : Buffer | null = null;
                let diffItemsOwnerId : Buffer | null = null;

                this._logger.silly("snapshotRow: ", snapshotRow);

                if (snapshotRow.base_snapshot_id)
                {
                    snapItemsOwnerId = snapshotRow.base_snapshot_id;
                    diffItemsOwnerId = this._target.snapshotId;
                }
                else
                {
                    snapItemsOwnerId = this._target.snapshotId;
                }

                this._logger.silly("SNAPSHOT ID: %s", BufferUtils.toStr(snapItemsOwnerId));

                const baseDbSnapshot = new DBSnapshot(snapItemsOwnerId!, snapshotRow.date!);
                return this._querySnapshotItems(snapItemsOwnerId!)
                    .then(items => {
                        baseDbSnapshot.addItems(items);

                        if (diffItemsOwnerId) {
                            this._logger.silly("DIFF ID: %s", BufferUtils.toStr(diffItemsOwnerId));

                            const diffDbSnapshot = new DBSnapshot(diffItemsOwnerId!, snapshotRow.date!);
                            diffDbSnapshot.cloneItemsFrom(baseDbSnapshot);

                            return this._queryDiffItems(diffItemsOwnerId!)
                                .then(diffItems => {
                                    for(const diffItem of diffItems)
                                    {
                                        if (diffItem.present)
                                        {
                                            diffDbSnapshot.addItem({
                                                dn: diffItem.dn,
                                                kind: diffItem.kind,
                                                config_kind: diffItem.config_kind,
                                                name: diffItem.name,
                                                config_hash: diffItem.config_hash!,
                                            });
                                        }
                                        else
                                        {
                                            diffDbSnapshot.deleteItem(diffItem);
                                        }
                                    }

                                    return {
                                        baseSnapshot: baseDbSnapshot,
                                        snapshot: diffDbSnapshot,
                                        summary: snapshotRow.summary
                                    }
                                })
                        } else {
                            return {
                                snapshot: baseDbSnapshot,
                                summary: snapshotRow.summary
                            }
                        }
                    })

            })
    }

    public querySnapshotRow()
    {
        return this._dataStore.table(this._context.dataStore.snapshots.Snapshots)
            .queryOne({
                part: this._partId,
                snapshot_id: this._snapshotId
            });
    }

    public querySnapshotSummary() : Promise<DeltaSummary | null>
    {
        return this._dataStore.table(this._context.dataStore.snapshots.Snapshots)
            .queryOne({
                part: this._partId,
                snapshot_id: this._snapshotId
            }, {
                fields: { fields: ['summary'] }
            })
            .then(row => {
                if (!row) {
                    return null
                }
                return <DeltaSummary | null>row.summary;
            });
    }


    private _querySnapshotItems(snapshotId: Buffer)
    {
        const partId = UuidUtils.getPartFromDatedUUIDBuf(snapshotId);
        return this._dataStore.table(this._context.dataStore.snapshots.SnapItems)
            .queryMany({
                part: partId,
                snapshot_id: snapshotId
            });
    }

    private _queryDiffItems(snapshotId: Buffer)
    {
        const partId = UuidUtils.getPartFromDatedUUIDBuf(snapshotId);
        return this._dataStore.table(this._context.dataStore.snapshots.DiffItems)
            .queryMany({
                part: partId,
                snapshot_id: snapshotId
            });
    }

}
