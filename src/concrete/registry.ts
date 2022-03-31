import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger';

import { ConcreteItem } from './item';
import yaml from 'js-yaml';

import { ItemId, IConcreteRegistry } from '@kubevious/helper-logic-processor'
export class ConcreteRegistry implements IConcreteRegistry
{
    private _logger : ILogger;
    private _snapshotId: string;
    private _date : Date;
    private _agentVersion: string;

    private _flatItemsDict : Record<any, ConcreteItem> = {};
    private _itemsKindDict : Record<any, Record<any, ConcreteItem>> = {};

    constructor(logger: ILogger, snapshotId: string, date: Date, agentVersion: string)
    {
        this._logger = logger.sublogger("ConcreteRegistry");
        this._snapshotId = snapshotId;
        this._date =  date;
        this._agentVersion = agentVersion;
    }

    get logger() : ILogger {
        return this._logger;
    }

    get snapshotId() {
        return this._snapshotId;
    }

    get date() {
        return this._date;
    }

    get agentVersion() {
        return this._agentVersion;
    }

    get allItems() : ConcreteItem[] {
        return _.values(this._flatItemsDict);
    }

    add(id: ItemId, obj: any)
    {
        this.logger.verbose("[add] ", id);

        const rawId = this._makeDictId(id);
        const item = new ConcreteItem(this, id, obj);

        this._flatItemsDict[rawId] = item;

        if (!this._itemsKindDict[item.groupKey]) {
            this._itemsKindDict[item.groupKey] = {}
        }
        this._itemsKindDict[item.groupKey][rawId] = item;
    }

    remove(id: ItemId)
    {
        this.logger.verbose("[remove] %s", id);

        const rawId = this._makeDictId(id);

        const item = this._flatItemsDict[rawId];
        if (item) {

            const groupDict = this._itemsKindDict[item.groupKey];
            if (groupDict) {
                delete groupDict[rawId];
                if (_.keys(groupDict).length !== 0)
                {
                    delete this._itemsKindDict[item.groupKey];
                }
            } else {
                this.logger.warn("[remove] Failed to remove kind group key %s for %s", item.groupKey, rawId);
            }

            delete this._flatItemsDict[rawId];
        }
    }

    findById(id: ItemId) : ConcreteItem | null
    {
        const rawId = this._makeDictId(id);
        const item = this._flatItemsDict[rawId];
        if (item) {
            return item;
        }
        return null;
    }

    filterItems(idFilter: any) : ConcreteItem[] {
        const result : ConcreteItem[] = [];
        for(const item of this.allItems) {
            if (item.matchesFilter(idFilter)) {
                result.push(item);
            }
        }
        return result;
    }

    private _makeDictId(id: ItemId) : string {
        if (_.isString(id)) {
            return id;
        }
        return _.stableStringify(id);
    }

    extractCapacity()
    {
        let cap = [];
        for(const groupKey of _.keys(this._itemsKindDict))
        {
            cap.push({
                name: groupKey,
                count: _.keys(this._itemsKindDict[groupKey]).length
            });
        }
        cap = _.orderBy(cap, ['count', 'name'], ['desc', 'asc']);
        return cap;
    }

    debugOutputCapacity()
    {
        this.logger.info("[concreteRegistry] >>>>>>>");
        this.logger.info("[concreteRegistry] Total Count: %s", _.keys(this._flatItemsDict).length);

        const counters = this.extractCapacity();
        for(const x of counters)
        {
            this.logger.info("[concreteRegistry] %s :: %s", x.name, x.count);
        }

        this.logger.info("[concreteRegistry] <<<<<<<");
    }


    debugOutputToFile()
    {
        const writer = this.logger.outputStream("dump-concrete-registry");
        if (!writer) {
            return Promise.resolve();
        }

        this.logger.info("[debugOutputToFile] BEGIN");

        const ids = _.keys(this._flatItemsDict);
        ids.sort();
        for(const id of ids) {
            writer.write('-) ' + id);
            const item = this._flatItemsDict[id];
            item.debugOutputToFile(writer);
            writer.newLine();
        }

        writer.newLine();
        writer.newLine();
        writer.write("******************************************");
        writer.write("******************************************");
        writer.write("******************************************");
        writer.newLine();
        writer.newLine();

        return Promise.resolve(writer.close())
            .then(() => {
                this.logger.info("[debugOutputToFile] END");
            });
    }

    dump()
    {
        const result : Record<any, any> = {};
        const ids = _.keys(this._flatItemsDict);
        ids.sort();
        for(const id of ids) {
            const item = this._flatItemsDict[id];
            result[id] = item.dump();
        }
        return result;
    }

    debugOutputRegistry(registryName: string)
    {
        for(const item of this.allItems)
        {
            const content = yaml.dump(item.config, { indent: 4 });
            let fileDir = `${registryName}`;
            if (item.config && item.config.synthetic) {
                fileDir = `${fileDir}/synthetic`;
            } else {
                fileDir = `${fileDir}/k8s`;
            }
            fileDir = `${fileDir}//${item.groupKey}`;

            let fileName = '';
            if (item.id.namespace) {
                fileName = `${item.id.namespace}-`;
            }
            fileName = `${fileName}${item.id.name}.yaml`

            const filePath = `${fileDir}/${fileName}`;

            this.logger.outputFile(filePath, content);
        }
    }
}
