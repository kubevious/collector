import _ from 'the-lodash';
import { Promise, Resolvable } from 'the-promise';
import { ILogger } from 'the-logger' ;

import moment from 'moment';

import { DateUtils } from '@kubevious/data-models';

import { K8sConfig, extractK8sConfigId } from '@kubevious/helper-logic-processor';

import { UuidUtils } from '@kubevious/data-models';

import { ReportableSnapshotItem, ResponseReportSnapshot, ResponseReportSnapshotItems } from '@kubevious/helpers/dist/reportable/types';
import { CollectorReportingInfo } from '@kubevious/data-models/dist/accessors/config-accessor';

import { CollectorSnapshotInfo, MetricItem } from './types';
import { ConcreteRegistry } from '../concrete/registry';

import { Context } from '../context';

const SNAPSHOT_QUEUE_SIZE = 5;
const SNAPSHOT_ACCEPT_DELAY_SECONDS = 3 * 60;

export class Collector
{
    private _logger : ILogger;
    private _context : Context

    private _snapshots : Record<string, CollectorSnapshotInfo> = {};
    private _snapshotsToProcess : Record<string, boolean> = {};

    private _agentVersion? : string;
    private _currentMetric : MetricItem | null = null;
    private _latestMetric : MetricItem | null = null;
    private _recentDurations : number[] = [];

    private _configHashes : Record<string, any> = {};

    private _lastReportDate : moment.Moment | null = null;

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger("Collector");

        this.logger.info("[constructed] ");
    }

    get logger() {
        return this._logger;
    }
    
    newSnapshot(date: Date, agentVersion: string, baseSnapshotId?: string) : Resolvable<ResponseReportSnapshot>
    {
        this._agentVersion = agentVersion;

        const canAccept = this._canAcceptNewSnapshot();
        if (!canAccept.success) {
            const delaySeconds = canAccept.delaySec || 60;
            this.logger.info("Postponing reporting for %s seconds", delaySeconds);

            return {
                delay: true,
                delaySeconds: delaySeconds
            };
        }

        const metric = this._newMetric(date, 'snapshot');

        let item_hashes : Record<string, string> = {};
        if (baseSnapshotId)
        {
            const baseSnapshot = this._snapshots[baseSnapshotId!];
            if (baseSnapshot) {
                item_hashes = _.clone(baseSnapshot.item_hashes);
            } else {
                return RESPONSE_NEED_NEW_SNAPSHOT;
            }
        }

        const id = UuidUtils.newDatedUUID();

        const snapshotInfo : CollectorSnapshotInfo = {
            id: id,
            reportDate: new Date(),
            date: date,
            agentVersion: agentVersion,
            metric: metric,
            item_hashes: item_hashes
        };

        this._snapshots[id] = snapshotInfo;

        this._lastReportDate = moment();

        return Promise.resolve()
            .then(() => {
                const reportingInfo : CollectorReportingInfo = {
                    snapshot_id: snapshotInfo.id,
                    date: snapshotInfo.date.toISOString(),
                    agent_version: snapshotInfo.agentVersion,
                };
                return this._context.configAccessor.setCollectorReportingInfo(reportingInfo);
            })
            .then(() => {
                return {
                    id: id
                };
            })

    }

    acceptSnapshotItems(snapshotId: string, items: ReportableSnapshotItem[])
    {
        const snapshotInfo = this._snapshots[snapshotId];
        if (!snapshotInfo) {
            return RESPONSE_NEED_NEW_SNAPSHOT;
        }

        const missingHashes : string[] = [];

        for (const item of items)
        {
            if (item.present)
            {
                snapshotInfo.item_hashes[item.idHash] = item.configHash!;

                if (!(item.idHash in this._configHashes)) {
                    missingHashes.push(item.configHash!)
                }
            }
            else
            {
                delete snapshotInfo.item_hashes[item.idHash];
            }
        }

        const response : ResponseReportSnapshotItems = {}
        if (missingHashes.length > 0)
        {
            response.needed_configs = missingHashes;
        }

        return response;
    }

    activateSnapshot(snapshotId: string)
    {
        if (_.keys(this._snapshotsToProcess).length > 0) {
            return RESPONSE_NEED_NEW_SNAPSHOT;
        }

        return this._context.tracker.scope("collector::activateSnapshot", (tracker) => {
            const snapshotInfo = this._snapshots[snapshotId];
            if (!snapshotInfo) {
                return RESPONSE_NEED_NEW_SNAPSHOT;
            }

            this._lastReportDate = moment();

            this._endMetric(snapshotInfo.metric);

            this.logger.info("[_acceptSnapshot] item count: %s", _.keys(snapshotInfo.item_hashes).length);
            this.logger.info("[_acceptSnapshot] metric: ", snapshotInfo.metric);
            
            const registry = new ConcreteRegistry(this._logger,
                snapshotInfo.id,
                snapshotInfo.date,
                snapshotInfo.agentVersion);
                
            for(const itemHash of _.keys(snapshotInfo.item_hashes))
            {
                const configHash = snapshotInfo.item_hashes[itemHash];
                const config = this._configHashes[configHash];
                const itemId = this._extractId(config);
                registry.add(itemId, config);
            }
            
            this._cleanup();

            this._snapshotsToProcess[snapshotInfo.id] = true;

            this._context.facadeRegistry.acceptConcreteRegistry(registry);

            // Use only for debugging.
            // registry.debugOutputRegistry(`source-snapshot/${snapshotId}`);

            return {};
        });
    }

    storeConfig(hash: string, config: any)
    {
        this._configHashes[hash] = config;
    }

    completeSnapshotProcessing(snapshotId: string)
    {
        this.logger.info("[completeSnapshotProcessing] snapshotId: %s", snapshotId);
        delete this._snapshotsToProcess[snapshotId];
    }

    private _extractId(config: any)
    {
        const c = <K8sConfig>config;
        return extractK8sConfigId(c);
    }

    private _cleanup()
    {
        const snapshots = _.orderBy(_.values(this._snapshots), x => x.date, ['desc']);
        const liveSnapshots = _.take(snapshots, SNAPSHOT_QUEUE_SIZE);
        const toDeleteSnapshots = _.drop(snapshots, SNAPSHOT_QUEUE_SIZE);

        for(const snapshot of toDeleteSnapshots) {
            delete this._snapshots[snapshot.id];
        }

        const configHashesList = liveSnapshots.map(x => _.values(x.item_hashes));

        const finalConfigHashes = <string[]>_.union.apply(null, configHashesList);

        const configHashesToDelete = _.difference(_.keys(this._configHashes), finalConfigHashes);

        for(const configHash of configHashesToDelete)
        {
            delete this._configHashes[configHash];
        }
    }

    private _canAcceptNewSnapshot() : { success: boolean, delaySec? : number}
    {
        if (_.keys(this._snapshotsToProcess).length > 0) {
            return { success: false, delaySec: 60 };
        }

        if (this._context.facadeRegistry.jobDampener.isBusy) {
            return { success: false, delaySec: 60 };
        }

        if (!this._context.database.isConnected) {
            return { success: false, delaySec: 30 };
        }

        // if (!this._context.historyProcessor.isDbReady) {
        //     return { success: false, delaySec: 30 };
        // }

        if (this._lastReportDate)
        {
            // this.logger.info("[_canAcceptNewSnapshot] Last Report Date: %s", this._lastReportDate.toISOString());
            const nextAcceptDate = moment(this._lastReportDate).add(SNAPSHOT_ACCEPT_DELAY_SECONDS, "seconds");

            const diff = nextAcceptDate.diff(moment(), "second");

            if (diff >= 5) {
                return { success: false, delaySec: diff };
            }
        }
        
        return { success: true };
    }


    extractMetrics()
    {
        const metrics : UserMetricItem[] = [];

        metrics.push({
            category: 'Collector',
            name: 'Parser Version',
            value: this._agentVersion ? this._agentVersion : 'unknown'
        })

        metrics.push({
            category: 'Collector',
            name: 'Recent Durations',
            value: JSON.stringify(this._recentDurations)
        })

        if (this._currentMetric && !this._currentMetric.dateEnd) {
            metrics.push({
                category: 'Collector',
                name: 'Current Report Date',
                value: this._currentMetric.dateStart
            })
    
            metrics.push({
                category: 'Collector',
                name: 'Current Report Kind',
                value: this._currentMetric.kind
            })

            const durationSeconds = DateUtils.diffSeconds(new Date(), this._currentMetric.dateStart);
            metrics.push({
                category: 'Collector',
                name: 'Current Report Duration(sec). Still collecting...',
                value: durationSeconds
            })
        }

        if (this._latestMetric) {
            metrics.push({
                category: 'Collector',
                name: 'Latest Report Date',
                value: this._latestMetric.dateStart
            })

            metrics.push({
                category: 'Collector',
                name: 'Latest Report Kind',
                value: this._latestMetric.kind
            })

            if (this._latestMetric.durationSeconds) {
                metrics.push({
                    category: 'Collector',
                    name: 'Latest Report Duration(sec)',
                    value: this._latestMetric.durationSeconds
                })
            }
        }

        return metrics;
    }

    private _newMetric(date: Date, kind: string) 
    {
        const metric : MetricItem = {
            origDate: date,
            dateStart: new Date(),
            dateEnd: null,
            kind: kind,
            durationSeconds: null
        };
        this._currentMetric = metric;
        return metric;
    }

    private _endMetric(metric: MetricItem)
    {
        metric.dateEnd = new Date();
        metric.durationSeconds = DateUtils.diffSeconds(metric.dateEnd, metric.dateStart);
        this._recentDurations.push(metric.durationSeconds);
        this._recentDurations = _.takeRight(this._recentDurations, 10);
        this._latestMetric = metric;
        return metric;
    }

}

const RESPONSE_NEED_NEW_SNAPSHOT = {
    new_snapshot: true
};


interface UserMetricItem
{
    category: string,
    name: string,
    value: string | number | Date
}