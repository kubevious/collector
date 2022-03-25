import { AlertCounter } from "@kubevious/state-registry";

export interface DeltaSummary
{
    snapshot: SnapshotSummary,
    delta: SnapshotSummary,
}

export interface SnapshotSummary
{
    items: number,
    kinds: Record<string, number>,
    alerts: AlertCounter,
    alertsByKind: Record<string, AlertCounter>
}

export type AlertsSummary = Record<string, Record<string, AlertCounter > >;

export function newAlertsCounter() : AlertCounter
{
    return {
        error: 0,
        warn: 0
    }
}

export function newSnapshotSummary() : SnapshotSummary {
    const summary : SnapshotSummary = {
        items: 0,
        kinds: {},
        alerts: newAlertsCounter(),
        alertsByKind: {}
    };
    return summary;
}

export function newDeltaSummary() : DeltaSummary {
    const deltaSummary : DeltaSummary = {
        snapshot: newSnapshotSummary(),
        delta: newSnapshotSummary()
    }
    return deltaSummary;
}

export interface TimelineSummary
{
    changes: number,
    error: number,
    warn: number
}