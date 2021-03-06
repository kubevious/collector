export interface MetricItem
{
    origDate: Date,
    dateStart: Date,
    dateEnd: Date | null,
    kind: string,
    durationSeconds: number | null
}

export interface CollectorSnapshotInfo
{
    id: string,
    reportDate: Date,
    date: Date,
    agentVersion: string,
    metric: MetricItem,
    item_hashes: Record<string, string>,
}
