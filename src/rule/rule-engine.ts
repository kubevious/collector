import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { Context } from '../context';

import { RegistryState } from '@kubevious/state-registry';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

import { RulesProcessor, RuleObject } from '@kubevious/helper-rule-engine';

import { ITableAccessor } from '@kubevious/easy-data-store';

export class RuleEngine
{
    private _logger : ILogger;
    private _context : Context;
    private _dataStore : ITableAccessor;

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger("RuleProcessor");
        this._dataStore = context.dataStore.dataStore;
    }

    get logger() {
        return this._logger;
    }

    execute(state : RegistryState, rules: RuleObject[], tracker : ProcessingTrackerScoper)
    {
        this._logger.info("[execute] date: %s, count: %s", 
            state.date.toISOString(),
            state.getCount())

        return Promise.resolve()
            .then(() => {
                const processor = new RulesProcessor(this._logger, rules)
                return processor.execute(state, tracker)
            })
            .then(executionContext => {
                this.logger.info('[execute] END');
                return executionContext;
            })
    }
    
}
