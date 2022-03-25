import _ from 'the-lodash';
import { ILogger } from 'the-logger';

import { PersistableSnapshot } from '../executor/persistable-snapshot';
import { AlertsSummary, DeltaSummary, newAlertsCounter, newSnapshotSummary, SnapshotSummary, TimelineSummary } from './types';
import { DBSnapshotItemKey } from '../reader/types';
import { AlertCounter } from '@kubevious/state-registry';

export class SummaryCalculator
{
    private _logger: ILogger;
    private _targetSnapshot: PersistableSnapshot;
    private _deltaSnapshot: PersistableSnapshot;

    private _prevSummary: DeltaSummary;
    private _alertsSummary: AlertsSummary = {};
    private _timelineSummary: TimelineSummary = {
        changes: 0,
        error: 0,
        warn: 0
    }

    constructor(logger: ILogger, targetSnapshot: PersistableSnapshot, deltaSnapshot: PersistableSnapshot, prevSummary: DeltaSummary)
    {
        this._logger = logger.sublogger('SummaryCalculator');
        this._targetSnapshot = targetSnapshot;
        this._deltaSnapshot = deltaSnapshot;
        this._prevSummary = prevSummary;
    }

    get alertsSummary() {
        return this._alertsSummary;
    }

    get timelineSummary() {
        return this._timelineSummary;
    }

    process() : DeltaSummary
    {
        this._logger.info('[process] isDiffSnapshot: %s', this._deltaSnapshot.isDiffSnapshot);

        let deltaItems : Partial<DBSnapshotItemKey>[];
        if (this._deltaSnapshot.isDiffSnapshot) {
            deltaItems = this._deltaSnapshot.diffItems;
        } else {
            deltaItems = this._deltaSnapshot.dbSnapshot.getItems();
        }

        this._logger.info('[process] deltaItems count: %s', deltaItems.length);

        const deltaSummary : DeltaSummary = {
            snapshot: this._constructSnapshotSummary(this._targetSnapshot.dbSnapshot.getItems()),
            delta: this._constructSnapshotSummary(deltaItems)
        }

        this._alertsSummary = this._constructAlertsSummary();
        
        this._calculateCurrentSnapshotAlerts(deltaSummary.snapshot)

        this._calculateDeltaSnapshotAlerts(deltaSummary.snapshot, deltaSummary.delta)

        this._calculateTimelineSummary(deltaSummary);

        return deltaSummary;
    }

    private _constructSnapshotSummary(items: Partial<DBSnapshotItemKey>[]) : SnapshotSummary
    {
        const dns : Record<string, boolean> = {};
        const summary : SnapshotSummary = newSnapshotSummary();

        for(const item of items.filter(x => x.config_kind != 'alerts'))
        {
            if (!dns[item.dn!])
            {
                dns[item.dn!] = true;
                
                summary.items = summary.items + 1;

                if (!summary.kinds[item.kind!])
                {
                    summary.kinds[item.kind!] = 1;
                }
                else
                {
                    summary.kinds[item.kind!] = summary.kinds[item.kind!] + 1;
                }
            }
        }

        return summary;
    }

    private _constructAlertsSummary() : AlertsSummary
    {
        const alertsDict : AlertsSummary = {};
        for(const node of this._targetSnapshot.nodeConfigs)
        {
            const kind = node.config.kind;
            if (node.config.selfAlertCount)
            {
                const selfAlertsDict = <Record<string, number >> <any>node.config.selfAlertCount;
                for(const severity of _.keys(selfAlertsDict))
                {
                    const count = selfAlertsDict[severity];
                    if (count > 0)
                    {
                        if (!alertsDict[kind])
                        {
                            alertsDict[kind] = {};
                        }
                        if (!alertsDict[kind][node.dn])
                        {
                            alertsDict[kind][node.dn] = newAlertsCounter();
                        }
                        (<Record<string, number >><any>alertsDict[kind][node.dn])[severity] = count;
                    }
                }
            }
        }
        return alertsDict;
    }

    private _calculateCurrentSnapshotAlerts(snapshotSummary: SnapshotSummary)
    {
        const currentTotalAlerts = newAlertsCounter();
        const currentByKindAlerts : Record<string, AlertCounter> = {};
        for(const kind of _.keys(this._alertsSummary))
        {
            const dict = this._alertsSummary[kind];
            currentByKindAlerts[kind] = newAlertsCounter();
            for(const itemAlerts of _.values(dict))
            {
                this._appendAlertCounts(currentTotalAlerts, itemAlerts);
                this._appendAlertCounts(currentByKindAlerts[kind], itemAlerts);
            }
        }

        snapshotSummary.alerts = currentTotalAlerts;
        snapshotSummary.alertsByKind = currentByKindAlerts;
    }

    private _calculateDeltaSnapshotAlerts(snapshotSummary: SnapshotSummary, deltaSummary: SnapshotSummary)
    {
        const deltaAlertsDict = _.cloneDeep(snapshotSummary.alerts);
        const deltaAlertsByKindDict = _.cloneDeep(snapshotSummary.alertsByKind);

        this._subtractAlertCounts(deltaAlertsDict, this._prevSummary.snapshot.alerts);
        for(const kind of _.keys(this._prevSummary.snapshot.alertsByKind))
        {
            const dict = this._prevSummary.snapshot.alertsByKind[kind];
            if (!deltaAlertsByKindDict[kind]) {
                deltaAlertsByKindDict[kind] = newAlertsCounter();
            }
            this._subtractAlertCounts(deltaAlertsByKindDict[kind], dict);
        }

        for(const kind of _.keys(deltaAlertsByKindDict))
        {
            const dict = deltaAlertsByKindDict[kind];
            if (dict.error === 0 && dict.warn === 0) {
                delete deltaAlertsByKindDict[kind];
            }
        }

        deltaSummary.alerts = deltaAlertsDict;
        deltaSummary.alertsByKind = deltaAlertsByKindDict;
    }

    private _calculateTimelineSummary(deltaSummary: DeltaSummary)
    {
        this._timelineSummary.changes = deltaSummary.delta.items;
        this._timelineSummary.error = deltaSummary.snapshot.alerts.error;
        this._timelineSummary.warn = deltaSummary.snapshot.alerts.warn;
    }

    private _appendAlertCounts(counter: AlertCounter, other: AlertCounter)
    {
        counter.error += other.error;
        counter.warn += other.warn;
    }

    private _subtractAlertCounts(counter: AlertCounter, other: AlertCounter)
    {
        counter.error -= other.error;
        counter.warn -= other.warn;
    }

}