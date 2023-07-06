import _ from 'the-lodash';
import { ILogger } from 'the-logger' ;

import { Context } from '../context';
import { ConcreteRegistry } from '../concrete/registry';
import { JobDampener } from '@kubevious/helpers';
import { JobDampenerState } from '@kubevious/helpers/dist/job-dampener';

import { CollectorStateInfo } from '@kubevious/data-models/dist/accessors/config-accessor';

export class FacadeRegistry
{
    private _logger : ILogger;
    private _context : Context

    private _jobDampener : JobDampener<ConcreteRegistry>;
    private _latestDampenerState: JobDampenerState | null = null;

    constructor(context : Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger("FacadeRegistry");

        this._jobDampener = new JobDampener<ConcreteRegistry>(
            this._logger.sublogger("FacadeDampener"),
            this._processConcreteRegistry.bind(this),
            {
                queueSize: 1,
                rescheduleTimeoutMs: 1000,
                stateMonitorCb: this._jobDampenerStateMonitorCb.bind(this)
            });

    }

    get logger() {
        return this._logger;
    }

    get debugObjectLogger() {
        return this._context.debugObjectLogger;
    }

    get jobDampener() {
        return this._jobDampener;
    }

    init()
    {
        this._context.dataStore.onConnect(this._onDbConnect.bind(this));
    }

    acceptConcreteRegistry(registry: ConcreteRegistry)
    {
        this.logger.info('[acceptConcreteRegistry] count: %s', registry.allItems.length);
        this._jobDampener.acceptJob(registry);
    }

    private _processConcreteRegistry(registry: ConcreteRegistry, date: Date)
    {
        this._logger.info("[_processConcreteRegistry] Date: %s. Item count: %s, Snapshot: %s", date.toISOString(), registry.allItems.length, registry.snapshotId);

        return this._context.executor.process({ 
            registry: registry,
        })
        ;
    }

    private _jobDampenerStateMonitorCb(state: JobDampenerState)
    {
        this._logger.info("[_jobDampenerStateMonitorCb] ", state);
        this._latestDampenerState = state;

        return Promise.resolve(null)
            .then(() => {
                if (this._context.dataStore.isConnected) {
                    return this._persistLatestJobProcessorState();
                } else {
                    this._logger.info("[_jobDampenerStateMonitorCb] NOT YET CONNECTED TO DB");
                }
            })
            .then(() => {
                return this._tryProcessHistoryCleanup();
            })
    }

    private _tryProcessHistoryCleanup()
    {
        if (!this._latestDampenerState) {
            return;
        }
        if (this._latestDampenerState.isProcessing) {
            return;
        }

        this._context.historyCleanupProcessor.tryProcess();
    }

    private _onDbConnect()
    {
        return this._persistLatestJobProcessorState();
    }

    private _persistLatestJobProcessorState()
    {
        if (!this._latestDampenerState) {
            return;
        }

        const info: CollectorStateInfo = {
            snapshots_in_queue: this._latestDampenerState.totalJobs ?? 0
        }

        return Promise.resolve()
            .then(() => this._context.configAccessor.setCollectorStateConfig(info))
            .then(() => this._context.webSocketUpdater.notifyReporter())
            ;
    }

}
