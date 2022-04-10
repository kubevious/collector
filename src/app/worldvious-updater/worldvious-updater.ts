import _ from 'the-lodash';
import { ILogger } from 'the-logger' ;
import { Context } from '../../context';
import { RegistryBundleState } from '@kubevious/state-registry';

export class WorldviousUpdater
{
    private _logger : ILogger;
    private _context : Context;

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('WorldviousUpdater');
    }

    get logger() {
        return this._logger;
    }

    process(bundle: RegistryBundleState)
    {
        const counters = this._extractCounters(bundle);
        this.logger.info("[COUNTERS] BEGIN");
        for(const counter of counters)
        {
            this.logger.info("[COUNTERS] %s => %s", counter.name, counter.count);
        }
        this.logger.info("[COUNTERS] END");

        this._context.worldvious.acceptCounters(counters);
    }

    private _extractCounters(bundle: RegistryBundleState)
    {
        const nodeCountDict : Record<string, number> = {};
        for(const node of bundle.nodeItems)
        {
            if (!nodeCountDict[node.kind])
            {
                nodeCountDict[node.kind] = 1;
            }
            else
            {
                nodeCountDict[node.kind]++;
            }
        }

        const nodeCounters = _.keys(nodeCountDict).map(x => ({
            name: x,
            count: nodeCountDict[x]
        }))

        return nodeCounters;
    }

}