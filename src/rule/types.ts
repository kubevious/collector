export interface RuleObject {
    name: string;
    hash: Buffer; //string;
    target: string;
    script: string;
}

export type RuleItem = Record<string, any>;
