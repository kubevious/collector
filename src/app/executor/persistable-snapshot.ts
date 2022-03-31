import _ from 'the-lodash';

import { BundledNodeConfig } from '@kubevious/state-registry/dist/registry-bundle-state';
import { BufferUtils } from '@kubevious/data-models';
import { HashUtils } from '@kubevious/data-models';

import { DBSnapshot, makeKey } from '../reader/snapshot';

export type PersistableConfig = any;
export interface PersistableSnapshotItemsRow {
    dn: string;
    kind: string;
    config_kind: string;
    name?: string;
    config_hash: Buffer;
}

export interface PersistableDiffItemsRow {
    dn: string;
    kind: string;
    config_kind: string;
    name?: string;
    present: boolean;
    config_hash?: Buffer;
}

export class PersistableSnapshot
{
    private _dbSnapshot : DBSnapshot;
    private _configs: Record<string, ConfigItem> = {};
    private _nodeConfigs: NodeConfigInfo[] = [];

    private _isDiffSnapshot = false;
    private _diffItems : Record<string, PersistableDiffItemsRow> = {};

    constructor(snapshotId: Buffer, date: string | Date)
    {
        this._dbSnapshot = new DBSnapshot(snapshotId, date);
    }

    get partitionId() {
        return this._dbSnapshot.partitionId!;
    }

    get snapshotId() {
        return this._dbSnapshot.snapshotId!;
    }

    get date() {
        return this.dbSnapshot.date;
    }

    get snapItemCount() {
        return this.dbSnapshot.count;
    }

    get dbSnapshot() {
        return this._dbSnapshot;
    }

    get configs() {
        return this._configs;
    }

    get nodeConfigs() {
        return this._nodeConfigs;
    }

    get diffKeys() {
        return _.keys(this._diffItems);
    }

    get diffItems() {
        return _.values(this._diffItems);
    }

    get diffItemCount() {
        return this.diffItems.length;
    }

    get isDiffSnapshot() {
        return this._isDiffSnapshot;
    }

    addItem(item: PersistableSnapshotItemsRow)
    {
        this._dbSnapshot.addItem(item);
    }

    addDiffItem(item: PersistableDiffItemsRow)
    {
        this._diffItems[makeKey(item)] = item;
    }

    addConfig(config: PersistableConfig)
    {
        const hash = HashUtils.calculateObjectHash(config);
        const hashStr = BufferUtils.toStr(hash!);

        this._configs[hashStr] = {
            hash: hash,
            hashStr: hashStr,
            config: config
        }

        return hash;
    }

    getConfig(hash: Buffer | string) : any | null
    {
        let hashStr : string;
        if (_.isString(hash)) {
            hashStr = <string>hash;
        } else {
            hashStr = BufferUtils.toStr(hash)
        }
        const config = this._configs[hashStr];
        if (config) {
            return config.config;
        }

        return null;
    }

    addNodeConfig(dn: string, config: BundledNodeConfig)
    {
        this._nodeConfigs.push({
            dn: dn,
            config: config
        });
    }

    constructDiff(id: Buffer) : PersistableSnapshot
    {
        const newSnapshot = new PersistableSnapshot(id, this.date);
        newSnapshot._isDiffSnapshot = true;
        newSnapshot._configs = this._configs;
        return newSnapshot;
    }

    export() : any
    {
        const contents = {
            snapshot: this.dbSnapshot.export(),
            diff: {
                item_count: this.diffKeys.length,
                diff_keys: this.diffKeys.sort(),
                diff_items: this.diffItems.map(x => {
                    return {
                        dn: x.dn,
                        kind: x.kind,
                        config_kind: x.config_kind,
                        name: x.name,
                        present: x.present,
                        config_hash: x.config_hash ? BufferUtils.toStr(x.config_hash!) : null
                    }
                })
            }
        };
        return contents;
    }

}

export interface NodeConfigInfo
{
    dn: string,
    config: BundledNodeConfig
}
export interface ConfigItem
{
    hash: Buffer,
    hashStr: string,
    config: PersistableConfig
}