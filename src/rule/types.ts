export interface RuleObject {
    name: string;
    hash: Buffer; //string;
    target: string;
    script: string;
}

export interface MarkerObject {
    name: string;
}

export type RuleItem = Record<string, any>;
