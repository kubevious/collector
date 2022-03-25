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
            .then(() => this._produceDeltaSnapshot(tracker))
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

        });
    }

    private _queryBaseSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("query-base-snapshot", (innerTracker) => {

        });
    }

    private _produceDeltaSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("produce-delta-snapshot", (innerTracker) => {

        });
    }

    private _producePersistableSnapshot(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("produce-persistable-snapshot", (innerTracker) => {

        });
    }

    private _calculateSummary(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("calculate-summary", (innerTracker) => {

        });
    }

    private _persist(tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("persist", (innerTracker) => {

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

}

interface DeltaSnapshotInfo
{
    deltaChangePerc: number,
    snapshot: PersistableSnapshot
}