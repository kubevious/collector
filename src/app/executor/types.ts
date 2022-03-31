import { ConcreteRegistry } from "../../concrete/registry";

export interface ExecutorTarget {
    registry: ConcreteRegistry;
}

export interface ExecutorTaskTarget {
    registry: ConcreteRegistry;
    snapshotId: Buffer;
    date: Date;
}
