import _ from 'the-lodash';
import { Promise, Resolvable } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { Context } from '../context';
import { RegistryBundleState } from '@kubevious/state-registry';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';
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
         });
    }

    private _jobDampenerStateMonitorCb(state: JobDampenerState)
    {
        this._logger.info("[_jobDampenerStateMonitorCb] ", state);
        this._latestDampenerState = state;

        if (this._context.dataStore.isConnected) {
            return this._persistLatestJobProcessorState();
        } else {
            this._logger.info("[_jobDampenerStateMonitorCb] NOT YET CONNECTED TO DB");
        }
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

        return this._context.configAccessor.setCollectorStateConfig(info);
    }

    private _runFinalize(bundle : RegistryBundleState, tracker: ProcessingTrackerScoper)
    {
        return Promise.resolve()
            .then(() => {
                return this._debugOutput(bundle);
            })
            .then(() => {
                this._produceCounters(bundle);
            })
            // .then(() => {
            //     return tracker.scope("websocket-update", () => {
            //         return this._context.websocket.accept(bundle);
            //     });
            // })
            // .then(() => {
            //     return tracker.scope("registry-accept", () => {
            //         return this._context.registry.accept(bundle);
            //     });
            // })
            // .then(() => {
            //     return tracker.scope("autocomplete-builder-accept", () => {
            //         return this._context.autocompleteBuilder.accept(bundle)
            //     })
            // })
            // .then(() => {
            //     return tracker.scope("search-accept", () => {
            //         return this._context.searchEngine.accept(bundle);
            //     });
            // })
            .then(() => {
                // return tracker.scope("history-accept", () => {
                //     return this._context.historyProcessor.accept(bundle);
                // });
            })
    }

    private _debugOutput(bundle : RegistryBundleState)
    {
        return;
        return Promise.resolve()
            .then(() => {

                const snapshotInfo = {
                    date: bundle.date.toISOString(),
                    items: <any[]>[]
                }

                for (const x of bundle.nodeItems)
                {
                    snapshotInfo.items.push({
                        dn: x.dn,
                        config_kind: 'node',
                        config: x.config
                    })

                    for(const propName of _.keys(x.propertiesMap))
                    {
                        snapshotInfo.items.push({
                            dn: x.dn,
                            config_kind: 'props',
                            name: propName,
                            config: x.propertiesMap[propName]
                        })
                    }

                    if (x.selfAlerts.length > 0)
                    {
                        snapshotInfo.items.push({
                            dn: x.dn,
                            config_kind: 'alerts',
                            config: x.selfAlerts
                        })
                    }
                }

                this.debugObjectLogger.dump("latest-bundle", 0, snapshotInfo)
                
            })
            ;
    }

    private _produceCounters(bundle: RegistryBundleState)
    {
        const counters = this._extractCounters(bundle);
        this.logger.info("[COUNTERS] BEGIN");
        for(const counter of counters)
        {
            this.logger.info("[COUNTERS] %s => %s", counter.name, counter.count);
        }
        this.logger.info("[COUNTERS] END");
        this._context.worldvious.acceptCounters(counters);
    }

    private _extractCounters(bundle: RegistryBundleState)
    {
        const nodeCountDict : Record<string, number> = {};
        for(const node of bundle.nodeItems)
        {
            if (!nodeCountDict[node.kind])
            {
                nodeCountDict[node.kind] = 1;
            }
            else
            {
                nodeCountDict[node.kind]++;
            }
        }

        const nodeCounters = _.keys(nodeCountDict).map(x => ({
            name: x,
            count: nodeCountDict[x]
        }))

        return nodeCounters;
    }

}
