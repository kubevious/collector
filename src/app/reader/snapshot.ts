import _ from 'the-lodash';
import { DBSnapshotItemKey } from './types'
import { SnapItemsRow } from '@kubevious/data-models/dist/models/snapshots';
import { UuidUtils } from '@kubevious/data-models';
import { BufferUtils } from '@kubevious/data-models';

export class DBSnapshot
{
    private _snapshotId: Buffer | null;
    private _partitionId: number | null;
    private _date : Date;
    private _items : Record<string, Partial<SnapItemsRow>> = {};

    constructor(snapshotId: Buffer | null, date: string | Date)
    {
        this._snapshotId = snapshotId;
        if (snapshotId) {
            this._partitionId = UuidUtils.getPartFromDatedUUIDBuf(snapshotId);
        } else {
            this._partitionId = null;
        }
        this._date = new Date(date);
    }

    get partitionId() {
        return this._partitionId;
    }

    get snapshotId() {
        return this._snapshotId;
    }

    get date() {
        return this._date;
    }

    get count() {
        return _.keys(this._items).length;
    }

    get keys() {
        return _.keys(this._items);
    }

    addItemByKey(key: string, item: Partial<SnapItemsRow>)
    {
        this._items[key] = item;
    }

    addItem(item: Partial<SnapItemsRow>)
    {
        this._items[makeKey(item)] = item;
    }

    addItems(items: Partial<SnapItemsRow>[])
    {
        for(const item of items)
        {
            this.addItem(item);
        }
    }

    deleteItem(item: Partial<DBSnapshotItemKey>)
    {
        this.delteById(makeKey(item));
    }

    delteById(id: string)
    {
        delete this._items[id];
    }

    getItems() 
    {
        return _.values(this._items);
    }

    getDict()
    {
        return _.cloneDeep(this._items);
    }

    findById(id: string)
    {
        const item = this._items[id];
        if (!item) {
            return null;
        }
        return item;
    }

    findItem(item: DBSnapshotItemKey)
    {
        return this.findById(makeKey(item));
    }

    cloneItemsFrom(other: DBSnapshot)
    {
        for(const key of _.keys(other._items))
        {
            this._items[key] = other._items[key];
        }
    }

    export() : any
    {
        const contents = {
            snapshot_id: this.snapshotId ? BufferUtils.toStr(this.snapshotId!) : null,
            item_count: this.keys.length,
            snapshot_keys: this.keys.sort(),
            snapshot_items: this.getItems().map(x => {
                return {
                    dn: x.dn,
                    kind: x.kind,
                    config_kind: x.config_kind,
                    name: x.name,
                    config_hash: BufferUtils.toStr(x.config_hash!)
                }
            })

        };
        return contents;
    }
}

export function makeKey(item: Partial<DBSnapshotItemKey>) : string {

    if (!item.dn) {
        throw new Error("MISSING DN");
    }
    if (!item.kind) {
        throw new Error("MISSING kind");
    }
    if (!item.config_kind) {
        throw new Error("MISSING config_kind");
    }

    const parts = [
        item.dn,
        item.kind,
        item.config_kind
    ]
    if (item.name) {
        parts.push(item.name);
    }

    return parts.join('-');
}