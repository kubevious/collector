import { PersistableSnapshot } from '../executor/persistable-snapshot';
import { DeltaSummary, TimelineSummary } from '../summary/types';
import { ExecutionContext as RuleEngineExecutionContext } from '@kubevious/helper-rule-engine';
import { DeltaAction } from '@kubevious/easy-data-store';
import { MarkerItemsRow, RuleItemsRow } from '@kubevious/data-models/dist/models/rule_engine';

import { RuleObject } from '../../rule/types';


export interface SnapshotPersistorTarget {
    snapshotId: Buffer,
    prevSnapshotId: Buffer | null,
    date: Date,
    snapshot: PersistableSnapshot,
    latestDelta: PersistableSnapshot,
    summary: DeltaSummary,
    timelineSummary: TimelineSummary,
    rules: RuleObject[],
    ruleEngineResult: RuleEngineExecutionContext
}

export interface SnapshotPersistorOutputData {
    deltaRuleItems: DeltaAction<RuleItemsRow>[],
    deltaMarkerItems: DeltaAction<MarkerItemsRow>[],
}


export const LATEST_SNAPSHOT_CONFIG_NAME = 'LATEST_SNAPSHOT';
export interface LatestSnapshotIdConfig  {
    snapshot_id: string;
}