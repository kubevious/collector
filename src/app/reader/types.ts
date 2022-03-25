import { DeltaSummary } from "../summary/types";
import { DBSnapshot } from "./snapshot";


export interface DBSnapshotRow {
    base_snapshot_id: Buffer | null,
    date: string,
    summary: DeltaSummary
}

export interface DBSnapshotItemKey {
    dn: string,
    kind: string,
    config_kind: string,
    name?: string,
}

export interface DBSnapshotFullData
{
    snapshot: DBSnapshot,
    summary: DeltaSummary
}

export interface DBSnapshotProcessableData
{
    baseSnapshot?: DBSnapshot,
    snapshot: DBSnapshot,
    summary: DeltaSummary
}


export interface SnapshotReaderTarget {
    snapshotId: Buffer,
}
