import _ from 'the-lodash';
import { ILogger } from 'the-logger';
import { Promise } from 'the-promise';

import * as Path from 'path';

import { LogicProcessor } from '@kubevious/helper-logic-processor'
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';
import { RegistryState, RegistryBundleState, SnapshotConfigKind } from '@kubevious/state-registry';

import { Context } from '../../context'
import { ExecutorTaskTarget } from './types';

// import { RecentBaseSnapshotReader } from '../reader/recent-base-snapshot-reader';
import { DBSnapshot } from '../reader/snapshot';
import { PersistableSnapshot } from './persistable-snapshot';

import * as BufferUtils from '@kubevious/helpers/dist/buffer-utils';
// import { SnapshotReader } from '../reader/snapshot-reader';
import { SummaryCalculator } from '../summary/calculator';
import { DeltaSummary, newDeltaSummary, TimelineSummary } from '../summary/types';
// import { SnapshotResult } from '../fetcher/types';
// import { RulesEngineFetcher } from '../rule-engine/fetcher';
// import { MarkerObject, RuleObject } from '../rule-engine/types';
import { ExecutionContext as RuleEngineExecutionContext } from '@kubevious/helper-rule-engine';
import { SnapshotPersistorOutputData, SnapshotPersistorTarget } from '../persistor/types';
// import { WebSocketNotifierTarget } from '../websocket-notifier/types';
// import { SearchEnginePersistorTarget } from '../search-engine/types';
import { SnapshotsRow } from '@kubevious/data-models/dist/models/snapshots';
import { ValidationConfig } from '@kubevious/entity-meta';

export class ExecutorTask
{
    private _context : Context;
    private _logger : ILogger;
    private _target: ExecutorTaskTarget;

    private _snapshotIdStr: string;
    
    private _targetBundleState? : RegistryBundleState;
    private _targetSnapshot? : PersistableSnapshot;
    private _latestSnapshot: DBSnapshot | null = null;
    private _baseSnapshot: DBSnapshot | null = null;
    private _latestSummary : DeltaSummary | null = null;
    private _deltaSummary: DeltaSummary | null = null;
    private _baseDeltaSnapshots : DeltaSnapshotInfo[] = [];
    private _finalPersistableSnapshot : PersistableSnapshot | null = null;
    private _latestSnapshotDelta : PersistableSnapshot | null = null;
    private _timelineSummary : TimelineSummary | null = null;

    private _registryState? : RegistryState;
    // private _rules? : RuleObject[];
    // private _markers? : MarkerObject[];
    private _ruleEngineResult?: RuleEngineExecutionContext;

    private _snapshotPersistorOutput?: SnapshotPersistorOutputData;
    private _snapshotDate: Date = new Date();

    private _snapshotRow? : Partial<SnapshotsRow>;
    private _validationConfig: Partial<ValidationConfig> = {};

    constructor(logger: ILogger, context : Context, target: ExecutorTaskTarget)
    {
        this._logger = logger;
        this._context = context;
        this._target = target;

        this._snapshotIdStr = BufferUtils.toStr(target.snapshotId);

        this.logger.info('snapshot: %s', this._snapshotIdStr);
    }

    get logger() {
        return this._logger;
    }

    execute(tracker: ProcessingTrackerScoper) : Promise<void>
    {
        this.logger.info("[execute] Begin");

        return Promise.resolve()
            .then(() => this._queryValidatorConfig(tracker))
            .then(() => this._executeLogicProcessor(tracker))
            .then(() => this._queryRules(tracker))
            .then(() => this._executeSnapshotProcessor(tracker))
            .then(() => this._queryBaseSnapshot(tracker))
            .then(() => this._checkBaseSnapshot(tracker))
            .then(() => this._processLatestDeltaSnapshot(tracker))
            .then(() => this._processBaseDeltaSnapshot(tracker))
            .then(() => this._producePersistableSnapshot(tracker))
            .then(() => this._calculateSummary(tracker))
            .then(() => this._persist(tracker))
            .then(() => this._notifyWebSocket(tracker))
            ;
    }


    private _queryValidatorConfig(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("query-validator-config", (innerTracker) => {

        });
    }

    private _executeLogicProcessor(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("run-logic-processor", (innerTracker) => {

            const logicProcessor = new LogicProcessor(
                this.logger,
                tracker,
                this._context.parserLoader,
                this._target.registry,
                {});
            return logicProcessor.process()
                .then(registryState => {
                    this.logger.info("[_executeLogicProcessor] End. Node Count: %s", registryState.getNodes().length)

                    this._registryState = registryState;

                    // this.logger.info("LogicProcessor Complete.")
                    // this.logger.info("RegistryState Item Count: %s", registryState.getCount());

                })

        });
    }

    private _queryRules(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("query-rules", (innerTracker) => {

        });
    }

    private _executeSnapshotProcessor(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("run-snapshot-processor", (innerTracker) => {
            
            return this._context.snapshotProcessor.process(this._registryState!, tracker)
                .then(result => {
                    this.logger.info("SnapshotProcessor Complete.")
                    this.logger.info("SnapshotProcessor Count: %s", result.bundle.getCount())

                    this._targetBundleState = result.bundle;

                    this._targetSnapshot = this._produceSnapshot(result.bundle);
                    this.logger.info("SnapshotProcessor Target Item Count: %s", this._targetSnapshot.snapItemCount)
                    this.logger.info("SnapshotProcessor Target Partition: %s", this._targetSnapshot.partitionId)

                    // TODO: 
                    this._ruleEngineResult = result.ruleEngineResult;

                    return Promise.resolve()
                        .then(() => this._outputFile(`snapshot-processor-rules-engine-result.json`, result.ruleEngineResult))
                        .then(() => this._outputFile(`target-snapshot.json`, this._targetSnapshot!.export()));
                })
        });
    }

    private _queryBaseSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("query-base-snapshot", (innerTracker) => {

            this._latestSnapshot = new DBSnapshot(null, new Date());
            this._latestSummary = newDeltaSummary();

        });
    }

    private _checkBaseSnapshot(tracker: ProcessingTrackerScoper)
    {
        if (this._baseSnapshot) {
            this.logger.info("BaseSnapshot Item Count: %s, Id: %s", this._baseSnapshot.count, BufferUtils.toStr(this._baseSnapshot.snapshotId!))
        } else {
            this.logger.info("No BaseSnapshot")
        }

        if (!this._latestSnapshot) {
            throw new Error("Latest Snapshot Not Set");
        }

        return Promise.resolve()
            .then(() => {
                if (this._baseSnapshot) {
                    return this._outputFile(`base-snapshot.json`, this._baseSnapshot.export());
                }
            })
            .then(() => {
                return this._outputFile(`latest-snapshot.json`, this._latestSnapshot!.export());
            })
            .then(() => {
                return this._outputFile(`latest-summary.json`, this._latestSummary!)
            })
    }

    private _processLatestDeltaSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("process-latest-delta-snapshot", (innerTracker) => {
            
            const deltaSnapshot = this._produceDeltaSnapshot(this._latestSnapshot!); 

            this.logger.info("LatestDeltaSnapshot. Partition: %s", deltaSnapshot.partitionId)
            this.logger.info("LatestDeltaSnapshot. SNAP ITEMS: %s", deltaSnapshot.snapItemCount)
            this.logger.info("LatestDeltaSnapshot. DIFF ITEMS: %s", deltaSnapshot.diffItemCount)
            this.logger.info("LatestDeltaSnapshot. DIFF ITEMS PRESENT: %s", deltaSnapshot.diffItems.filter(x => x.present).length)
            this.logger.info("LatestDeltaSnapshot. DIFF ITEMS NOT PRESENT: %s", deltaSnapshot.diffItems.filter(x => !x.present).length)

            this._latestSnapshotDelta = deltaSnapshot;

            return Promise.resolve()
                .then(() => this._outputFile(`latest-delta-snapshot.json`, deltaSnapshot.export()));

        });
    }

    private _processBaseDeltaSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("process-base-delta-snapshot", (innerTracker) => {
         
            if (!this._baseSnapshot) {
                if (this._latestSnapshot!.snapshotId) {

                    if (this._targetSnapshot!.partitionId === this._latestSnapshotDelta!.partitionId) {
                        this._baseDeltaSnapshots.push({
                            deltaChangePerc: 0,
                            snapshot: this._latestSnapshotDelta!
                        })
                    } else {
                        this.logger.info("BaseDeltaSnapshot. Skipping latest snapshot because different partitionId.");
                    }
                }
                return;
            }

            if (this._baseSnapshot!.snapshotId == this._latestSnapshot!.snapshotId) {
                throw new Error("THIS SHOULD NOT HAPPEN.")
                return;
            }

            const deltaSnapshot = this._produceDeltaSnapshot(this._baseSnapshot!); 

            this.logger.info("BaseDeltaSnapshot. Partition: %s", deltaSnapshot.partitionId)
            this.logger.info("BaseDeltaSnapshot. SNAP ITEMS: %s", deltaSnapshot.snapItemCount)
            this.logger.info("BaseDeltaSnapshot. DIFF ITEMS: %s", deltaSnapshot.diffItemCount)
            this.logger.info("BaseDeltaSnapshot. DIFF ITEMS PRESENT: %s", deltaSnapshot.diffItems.filter(x => x.present).length)
            this.logger.info("BaseDeltaSnapshot. DIFF ITEMS NOT PRESENT: %s", deltaSnapshot.diffItems.filter(x => !x.present).length)

            if (this._targetSnapshot!.partitionId === deltaSnapshot.partitionId) {
                this._baseDeltaSnapshots.push({
                    deltaChangePerc: 0,
                    snapshot: deltaSnapshot
                })
            } else {
                this.logger.info("BaseDeltaSnapshot. Skipping base snapshot because different partitionId.");
            }

            return Promise.resolve()
                .then(() => this._outputFile(`base-delta-snapshot.json`, deltaSnapshot.export()))
                ;

        });
    }

    private _producePersistableSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("produce-persistable-snapshot", (innerTracker) => {

            for(const deltaInfo of this._baseDeltaSnapshots)
            {
                deltaInfo.deltaChangePerc = this._calculateDiffPercentage(deltaInfo.snapshot);
                const idStr = deltaInfo.snapshot.dbSnapshot.snapshotId ? BufferUtils.toStr(deltaInfo.snapshot.dbSnapshot.snapshotId) : 'NONE';
                this.logger.info("DeltaSnapshot. ID: %s", idStr);
                this.logger.info("DeltaSnapshot. deltaChangePerc: %s%%", deltaInfo.deltaChangePerc);
            }

            const deltaSnapshots = this._baseDeltaSnapshots.filter(x => x.deltaChangePerc < 50);
            const finalDeltaSnapshot = _.minBy(deltaSnapshots, x => x.deltaChangePerc);

            let finalSnapshot : PersistableSnapshot;
            if (finalDeltaSnapshot)
            {
                this.logger.info("DeltaSnapshot. Storing using diff snapshot. Percentage: %s%%", finalDeltaSnapshot.deltaChangePerc)
                finalSnapshot = finalDeltaSnapshot.snapshot;
                this.logger.info("DeltaSnapshot. Storing using diff snapshot. BaseId: %s", BufferUtils.toStr(finalSnapshot.dbSnapshot.snapshotId!))
            }
            else
            {
                this.logger.info("DeltaSnapshot. Storing using new snapshot.")
                finalSnapshot = this._targetSnapshot!;
            }

            this.logger.info("FinalSnapshot. SNAP ITEMS: %s", finalSnapshot.snapItemCount)
            this.logger.info("FinalSnapshot. DIFF ITEMS: %s", finalSnapshot.diffItemCount)
            this.logger.info("FinalSnapshot. DIFF ITEMS PRESENT: %s", finalSnapshot.diffItems.filter(x => x.present).length)
            this.logger.info("FinalSnapshot. DIFF ITEMS NOT PRESENT: %s", finalSnapshot.diffItems.filter(x => !x.present).length)

            this._finalPersistableSnapshot = finalSnapshot;

            return Promise.resolve()
                .then(() => this._outputFile(`final-snapshot.json`, finalSnapshot.export()))
                ;
            
        });
    }

    private _calculateSummary(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("calculate-summary", (innerTracker) => {

            const calculator = new SummaryCalculator(this._logger, this._targetSnapshot!, this._latestSnapshotDelta!, this._latestSummary!)
            const summary = calculator.process();

            this._deltaSummary = summary;
            this._timelineSummary = calculator.timelineSummary;

            return Promise.resolve()
                .then(() => this._outputFile(`delta-summary.json`, this._deltaSummary!))

        });
    }

    private _persist(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist", (innerTracker) => {

            const target : SnapshotPersistorTarget = {
                snapshotId: this._target.snapshotId,
                date: this._snapshotDate,
                snapshot: this._finalPersistableSnapshot!,
                latestDelta: this._latestSnapshotDelta!,
                summary: this._deltaSummary!,
                timelineSummary: this._timelineSummary!,
                prevSnapshotId: this._latestSnapshot ? this._latestSnapshot!.snapshotId : null,
                rules: [], //this._rules!,
                ruleEngineResult: this._ruleEngineResult!
            }

            return this._context.snapshotPersistor.persist(target, tracker)
                .then(result => {
                    this._snapshotPersistorOutput = result;

                    return this._outputFile(`snapshot-persistor-result.json`, this._snapshotPersistorOutput);
                })

        });
    }

    private _notifyWebSocket(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("notify-websocket", (innerTracker) => {

        });
    }

    private _markComplete(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("", (innerTracker) => {

        });
    }


    private _produceSnapshot(state: RegistryBundleState) : PersistableSnapshot
    {
        const snapshot = new PersistableSnapshot(this._target.snapshotId, state.date);
        
        this._logger.info("[_produceSnapshot] date: %s, count: %s", snapshot.date.toISOString(), state.getCount());

        for(const node of state.nodeItems)
        {
            {
                const configHash = snapshot.addConfig(node.config);

                snapshot.addItem({
                    config_kind: SnapshotConfigKind.node,
                    dn: node.dn,
                    kind: node.kind,
                    config_hash: configHash
                });
                
                snapshot.addNodeConfig(node.dn, node.config);
            }

            {
                const childrenNodes = state.getChildren(node.dn);
                if (childrenNodes.length > 0)
                {
                    let childrenRns = childrenNodes.map(x => x.rn);
                    childrenRns = childrenRns.sort();

                    const configHash = snapshot.addConfig(childrenRns);

                    snapshot.addItem({
                        config_kind: SnapshotConfigKind.children,
                        dn: node.dn,
                        kind: node.kind,
                        config_hash: configHash
                    });
                }
            }
            
            {
                for(const props of _.values(node.propertiesMap))
                {
                    const configHash = snapshot.addConfig(props);

                    snapshot.addItem({
                        config_kind: SnapshotConfigKind.props,
                        dn: node.dn,
                        kind: node.kind,
                        name: props.id,
                        config_hash: configHash
                    });
                }
            }

            {
                if (node.selfAlerts.length > 0)
                {
                    const configHash = snapshot.addConfig(node.selfAlerts);

                    snapshot.addItem({
                        config_kind: SnapshotConfigKind.alerts,
                        dn: node.dn,
                        kind: node.kind,
                        config_hash: configHash
                    });
                }
            }
        }

        return snapshot;
    }


    private _produceDeltaSnapshot(sourceSnapshot: DBSnapshot) : PersistableSnapshot
    {
        if (sourceSnapshot.snapshotId) {
            this._logger.info('[_produceDeltaSnapshot] Begin, SourceID: %s', BufferUtils.toStr(sourceSnapshot.snapshotId!) );
        } else {
            this._logger.info('[_produceDeltaSnapshot] Begin, No SourceID.');
        }

        const targetDbSnapshot = this._targetSnapshot!.dbSnapshot;
        const diffSnapshot = this._targetSnapshot!.constructDiff(sourceSnapshot.snapshotId!);

        this._logger.info('[_produceDeltaSnapshot] baseSnapshot size: %s', sourceSnapshot.count);
        this._logger.info('[_produceDeltaSnapshot] targetDbSnapshot size: %s', targetDbSnapshot.count);

        for(const key of targetDbSnapshot.keys)
        {
            const item = targetDbSnapshot.findById(key)!;
            const baseItem = sourceSnapshot.findById(key);
            if (baseItem)
            {
                if (!BufferUtils.areEqual(item.config_hash!, baseItem.config_hash!))
                {
                    diffSnapshot.addDiffItem({
                        dn: item.dn!,
                        kind: item.kind!,
                        config_kind: item.config_kind!,
                        name: item.name,
                        config_hash: item.config_hash!,
                        present: true
                    })
                }
            }
            else
            {
                diffSnapshot.addDiffItem({
                    dn: item.dn!,
                    kind: item.kind!,
                    config_kind: item.config_kind!,
                    name: item.name,
                    config_hash: item.config_hash!,
                    present: true
                })
            }
        }

        for(const key of sourceSnapshot.keys)
        {
            const item = targetDbSnapshot.findById(key);
            if (!item)
            {
                const baseItem = sourceSnapshot.findById(key)!;

                diffSnapshot.addDiffItem({
                    dn: baseItem.dn!,
                    kind: baseItem.kind!,
                    config_kind: baseItem.config_kind!,
                    name: baseItem.name,
                    present: false
                })
            }
        }

        return diffSnapshot;
    }

    private _calculateDiffPercentage(deltaSnapshot: PersistableSnapshot) : number
    {
        if (!deltaSnapshot.dbSnapshot.snapshotId) {
            return 0;
        }

        if (this._targetSnapshot!.snapItemCount == 0) {
            return 0;
        }

        return Math.round(deltaSnapshot.diffItemCount * 100 / this._targetSnapshot!.snapItemCount);
    }

    private _outputFile(fileName: string, contents: any)
    {
        const filePath = Path.join(
            `snapshot-${this._snapshotIdStr}`,
            fileName
        )
        return this.logger.outputFile(filePath, contents)
    }

}

interface DeltaSnapshotInfo
{
    deltaChangePerc: number,
    snapshot: PersistableSnapshot
}