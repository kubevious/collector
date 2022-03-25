import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { ProcessingTrackerScoper } from '@kubevious/helper-backend';
import { Context } from '../../context'

import { LatestSnapshotIdConfig, LATEST_SNAPSHOT_CONFIG_NAME, SnapshotPersistorOutputData, SnapshotPersistorTarget } from './types'
import { ITableAccessor, ITableDriver, MySqlDriver } from '@kubevious/easy-data-store';

import { ConfigItem } from '../executor/persistable-snapshot';
import { DeltaItemsRow, DiffItemsRow, SnapItemsRow, SnapshotsRow } from '@kubevious/data-models/dist/models/snapshots';
import { MarkerItemsRow, RuleClusterStatusRow, RuleItemsRow, RuleLogsRow } from '@kubevious/data-models/dist/models/rule_engine';

import * as UuidUtils from '@kubevious/data-models/dist/utils/uuid-utils';
import * as BufferUtils from '@kubevious/helpers/dist/buffer-utils';


export class SnapshotPersistorTask
{
    private _logger : ILogger;
    private _context : Context;
    private _target: SnapshotPersistorTarget;

    private _dataStore : ITableAccessor;
    private _partitionId : number;
    private _partitionIdValue : number;

    private _currentConfigHashes : Record<string, boolean> = {};
    private _configHashDelta : ConfigItem[] = [];

    private _outputData : SnapshotPersistorOutputData;

    

    constructor(logger: ILogger, context : Context, target: SnapshotPersistorTarget)
    {
        this._context = context;
        this._logger = logger;

        this._target = target;

        this._partitionId = UuidUtils.getPartFromDatedUUIDBuf(target.snapshotId);
        this._partitionIdValue = this._partitionId + 1;

        this._dataStore = context.dataStore.dataStore;

        this._outputData = {
            deltaRuleItems: [],
            deltaMarkerItems: [],
        };
    }

    get logger() {
        return this._logger;
    }

    execute(tracker: ProcessingTrackerScoper)
    {
        this.logger.info("[execute] count %s", this._target.snapshot.snapItemCount);

        const tables = [
            this._context.dataStore.config.Config,
            this._context.dataStore.snapshots.SnapshotConfigs,
            this._context.dataStore.snapshots.SnapItems,
            this._context.dataStore.snapshots.DiffItems,
            this._context.dataStore.snapshots.DeltaItems,
            this._context.dataStore.snapshots.Snapshots,
            this._context.dataStore.snapshots.Timeline
        ];

        const tableNames = tables.map(x => x.tableName);

        return this._context.dataStore.executeInTransaction(tableNames, () => {
            return Promise.resolve()
                .then(() => this._preparePartitions(tracker))
                .then(() => {
                    return tracker.scope("configs", (innerTracker) => {
                        return this._persistConfigs(innerTracker);
                    });
                })
                .then(() => this._persistSnapshotItems(tracker))
                .then(() => this._persistDiffItems(tracker))
                .then(() => this._persistDeltaItems(tracker))
                .then(() => this._persistTimeline(tracker))
                .then(() => this._persistRuleEngine(tracker))
                .then(() => this._persistSnapshot(tracker))
                .then(() => this._persistSnapshotIndex(tracker))
        })
        .then(() => this._outputData)
    }

    private _preparePartitions(tracker: ProcessingTrackerScoper)
    {
        const tables = [
            this._context.dataStore.snapshots.Snapshots,
            this._context.dataStore.snapshots.SnapItems,
            this._context.dataStore.snapshots.DiffItems,
            this._context.dataStore.snapshots.DeltaItems,
            this._context.dataStore.snapshots.SnapshotConfigs,
            this._context.dataStore.snapshots.Timeline
        ];

        return Promise.serial(tables, x => this._preparePartition(x.table(), tracker));
    }

    private _preparePartition(table : ITableDriver<any>, tracker: ProcessingTrackerScoper)
    {
        const partitionManager = (<MySqlDriver>table.driver).partitionManager;
        return partitionManager.queryPartitions(table.name)
            .then(partitions => {
                // this.logger.info("PARTITIONS: ", partitions);
                const partition = _.find(partitions, x => x.value == this._partitionIdValue);
                if (partition) {
                    return;
                }

                return partitionManager.createPartition(table.name, `p${this._partitionId}`, this._partitionIdValue);
            });
    }

    private _persistConfigs(tracker: ProcessingTrackerScoper)
    {
        return Promise.resolve()
            .then(() => {
                return tracker.scope("fetch", (innerTracker) => {
                    return this._queryCurrentConfigs(innerTracker);
                });
            })
            .then(() => {
                return tracker.scope("delta", (innerTracker) => {
                    return this._produceConfigsDelta(innerTracker);
                });
            })
            .then(() => {
                return tracker.scope("apply", (innerTracker) => {
                    return this._persistConfigDeltas(innerTracker);
                });
            })
    }

    private _queryCurrentConfigs(tracker: ProcessingTrackerScoper)
    {
        return this._dataStore.table(this._context.dataStore.snapshots.SnapshotConfigs)
            .queryMany({
                part: this._partitionId
            }, {
                fields: { fields: ['hash' ]}
            })
            .then(rows => {
                this._currentConfigHashes = _.makeDict(rows, x => BufferUtils.toStr(x.hash!), x => true);
                this.logger.info("[_queryCurrentConfigs] Current Config Count: %s", _.keys(this._currentConfigHashes).length);
            });
    }

    private _produceConfigsDelta(tracker: ProcessingTrackerScoper)
    {
        this.logger.info("[_produceConfigsDelta] Target Config Count: %s", _.keys(this._target.snapshot.configs).length);

        this._configHashDelta = [];

        for(const config of _.values(this._target.snapshot.configs))
        {
            if (!this._currentConfigHashes[config.hashStr]) {
                this._configHashDelta.push(config);
            }
        }

        this.logger.info("[_produceConfigsDelta] Delta Config Count: %s", this._configHashDelta.length);
    }

    private _persistConfigDeltas(tracker: ProcessingTrackerScoper)
    {
        return Promise.execute(this._configHashDelta, item => {
            return this._persistConfig(tracker, item);
        }, {
            concurrency: 10
        })
    }

    private _persistConfig(tracker: ProcessingTrackerScoper, item: ConfigItem)
    {
        return this._dataStore.table(this._context.dataStore.snapshots.SnapshotConfigs)
            .createNew({
                part: this._partitionId,
                hash: item.hash, 
                value: item.config
            });
    }

    
    private _persistTimeline(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist-timeline", (innerTracker) => {

            return this._dataStore.table(this._context.dataStore.snapshots.Timeline)
                .create({
                    part: this._partitionId,
                    snapshot_id: this._target.snapshotId,
                    date: this._target.date,
                    ...this._target.timelineSummary,
                });

        });
    }

    private _persistSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist-snapshot", (innerTracker) => {

            const data : Partial<SnapshotsRow> = {
                part: this._partitionId,
                snapshot_id: this._target.snapshotId,
                date: this._target.date,
                prev_snapshot_id: this._target.prevSnapshotId!,
                summary: this._target.summary
            }

            if (this._target.snapshot.dbSnapshot.snapshotId) {
                data.base_snapshot_id = this._target.snapshot.dbSnapshot.snapshotId;
            }

            return this._dataStore.table(this._context.dataStore.snapshots.Snapshots)
                .create(data);
        });
    }

    private _persistSnapshotIndex(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist-index", (innerTracker) => {

            const valueObj : LatestSnapshotIdConfig = {
                snapshot_id: BufferUtils.toStr(this._target.snapshotId)
            };
    
            return this._context.dataStore.setConfig(LATEST_SNAPSHOT_CONFIG_NAME, valueObj);
        });
    }

    private _persistSnapshotItems(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("items", (innerTracker) => {
        
            return Promise.execute(this._target.snapshot.dbSnapshot.getItems(), item => {
                return this._persistSnapshotNode(item);
            }, {
                concurrency: 10
            });

        });
    }

    private _persistDiffItems(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("diffs", (innerTracker) => {
        
            return Promise.execute(this._target.snapshot.diffItems, item => {
                return this._persistDiffNode(item);
            }, {
                concurrency: 10
            });

        });
    }

    private _persistDeltaItems(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("delta", (innerTracker) => {
        
            return Promise.execute(this._target.latestDelta.diffItems, item => {
                return this._persistDeltaNode(item);
            }, {
                concurrency: 10
            });

        });
    }

    private _persistSnapshotNode(item: Partial<SnapItemsRow>)
    {
        return this._dataStore.table(this._context.dataStore.snapshots.SnapItems)
            .create({
                part: this._partitionId,
                snapshot_id: this._target.snapshotId,
                ...item
            });
    }

    private _persistDiffNode(item: Partial<DiffItemsRow>)
    {
        return this._dataStore.table(this._context.dataStore.snapshots.DiffItems)
            .create({
                part: this._partitionId,
                snapshot_id: this._target.snapshotId,
                ...item
            });
    }

    private _persistDeltaNode(item: Partial<DeltaItemsRow>)
    {
        return this._dataStore.table(this._context.dataStore.snapshots.DeltaItems)
            .create({
                part: this._partitionId,
                snapshot_id: this._target.snapshotId,
                ...item
            });
    }

    private _persistRuleEngine(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist-rule-engine", (innerTracker) => {

            return Promise.resolve()
                .then(() => this._persistRuleStatuses(innerTracker))
                .then(() => this._persistRuleItems(innerTracker))
                .then(() => this._persistRuleLogs(innerTracker))
                .then(() => this._persistMarkerItems(innerTracker))
                ;
          
        });
    }

    private _persistRuleStatuses(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("rule-statuses", () => {

            const targetItems : Partial<RuleClusterStatusRow>[] = [];

            for(const ruleObj of this._target.rules)
            {
                const ruleResult = this._target.ruleEngineResult.rules[ruleObj.name];
                
                targetItems.push({
                    rule_name: ruleObj.name,
                    hash: ruleObj.hash,
                    error_count: ruleResult ? ruleResult.error_count : 0,
                    item_count: ruleResult ? ruleResult.items.length : 0,
                })
            }

            return this._dataStore.table(this._context.dataStore.ruleEngine.RuleClusterStatuses)
                .synchronizer()
                .execute(targetItems);
        });
    }

    private _persistRuleItems(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("rule-items", () => {

            const targetItems : Partial<RuleItemsRow>[] = [];

            for(const rule of _.values(this._target.ruleEngineResult.rules))
            {
                for(const ruleItem of rule.items) 
                {
                    targetItems.push({
                        rule_name: rule.name,
                        dn: ruleItem.dn,
                        errors: ruleItem.errors,
                        warnings: ruleItem.warnings,
                        markers: ruleItem.markers
                    })
                }
            }

            return this._dataStore.table(this._context.dataStore.ruleEngine.RuleItems)
                .synchronizer()
                .execute(targetItems)
                .then(delta => {
                    this._outputData.deltaRuleItems = delta;
                });
        });
    }

    private _persistRuleLogs(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("rule-logs", () => {

            const targetItems : Partial<RuleLogsRow>[] = [];

            for(const rule of _.values(this._target.ruleEngineResult.rules))
            {
                for(const ruleLog of rule.logs) 
                {
                    targetItems.push({
                        rule_name: rule.name,
                        kind: ruleLog.kind,
                        msg: ruleLog.msg
                    })
                }
            }

            return this._dataStore.table(this._context.dataStore.ruleEngine.RuleLogs)
                .synchronizer()
                .execute(targetItems);
        });
    }

    private _persistMarkerItems(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("marker-items", () => {

            const targetItems : Partial<MarkerItemsRow>[] = [];

            for(const marker of _.values(this._target.ruleEngineResult.markers))
            {
                for(const dn of marker.items) 
                {
                    targetItems.push({
                        marker_name: marker.name,
                        dn: dn
                    })
                }
            }

            return this._dataStore.table(this._context.dataStore.ruleEngine.MarkerItems)
                .synchronizer()
                .execute(targetItems)
                .then(delta => {
                    this._outputData.deltaMarkerItems = delta;
                });
        });
    }

}