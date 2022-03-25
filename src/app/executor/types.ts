import { ConcreteRegistry } from "../../concrete/registry";

export interface ExecutorTarget {
    registry: ConcreteRegistry;
    snapshotId: string;
    date: Date;
}

export interface ExecutorTaskTarget {
    registry: ConcreteRegistry;
    snapshotId: Buffer
    date: Date;
}
