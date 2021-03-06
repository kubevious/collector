import { PersistableSnapshot } from '../executor/persistable-snapshot';
import { DeltaSummary, TimelineSummary } from '../summary/types';
import { ExecutionContext as RuleEngineExecutionContext } from '@kubevious/helper-rule-engine';
import { DeltaAction } from '@kubevious/easy-data-store';
import { MarkerItemsRow, RuleItemsRow } from '@kubevious/data-models/dist/models/rule_engine';

import { RuleObject } from '../../rule/types';
import { PersistenceItem } from '@kubevious/helper-logic-processor/dist/store/presistence-store';


export interface SnapshotPersistorTarget {
    snapshotId: Buffer,
    prevSnapshotId: Buffer | null,
    date: Date,
    agentVersion: string,
    snapshot: PersistableSnapshot,
    latestDelta: PersistableSnapshot,
    summary: DeltaSummary,
    timelineSummary: TimelineSummary,
    rules: RuleObject[],
    ruleEngineResult: RuleEngineExecutionContext,
    logicStoreItems: PersistenceItem[],
}

export interface SnapshotPersistorOutputData {
    deltaRuleItems: DeltaAction<RuleItemsRow>[],
    deltaMarkerItems: DeltaAction<MarkerItemsRow>[],
}

