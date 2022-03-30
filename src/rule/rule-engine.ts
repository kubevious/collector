import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { Context } from '../context';

import { RegistryState } from '@kubevious/state-registry';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

import { RulesProcessor, ExecutionContext } from '@kubevious/helper-rule-engine';
import { RuleStatusRow, RuleItemsRow, RuleLogsRow, MarkerItemsRow } from '@kubevious/data-models/dist/models/rule_engine';

import { RuleObject } from './types';
import { ISynchronizer } from '@kubevious/easy-data-store/dist/driver';
import { ITableAccessor } from '@kubevious/easy-data-store';

export class RuleEngine
{
    private _logger : ILogger;
    private _context : Context;
    private _dataStore : ITableAccessor;

    private _ruleStatusesSynchronizer : ISynchronizer<RuleStatusRow>;
    private _ruleItemsSynchronizer : ISynchronizer<RuleItemsRow>;
    private _ruleLogsSynchronizer : ISynchronizer<RuleLogsRow>;
    private _markerItemsSynchronizer : ISynchronizer<MarkerItemsRow>;

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger("RuleProcessor");
        this._dataStore = context.dataStore.dataStore;

        this._ruleStatusesSynchronizer = 
            this._dataStore.table(context.dataStore.ruleEngine.RuleStatuses)
                .synchronizer();

        this._ruleItemsSynchronizer = 
            this._dataStore.table(context.dataStore.ruleEngine.RuleItems)
                .synchronizer();

        this._ruleLogsSynchronizer = 
            this._dataStore.table(context.dataStore.ruleEngine.RuleLogs)
                .synchronizer();

        this._markerItemsSynchronizer = 
            this._dataStore.table(context.dataStore.ruleEngine.MarkerItems)
                .synchronizer();
    }

    get logger() {
        return this._logger;
    }

    execute(state : RegistryState, tracker : ProcessingTrackerScoper)
    {
        this._logger.info("[execute] date: %s, count: %s", 
            state.date.toISOString(),
            state.getCount())

        let rulesDict : Record<string, RuleObject> = {};

        return this._fetchRules()
            .then(rules => {
                rulesDict = _.makeDict(rules, x => x.name, x => x);
                const processor = new RulesProcessor(this._logger, rules)
                return processor.execute(state, tracker)
            })
            .then(executionContext => {
                return this._postProcess(executionContext, rulesDict);
            })
            .then(() => {
                this.logger.info('[execute] END');
            })
    }

    private _fetchRules() 
    {
        return this._dataStore.table(this._context.dataStore.ruleEngine.Rules)
            .queryMany({ enabled: true })
            .then(rows => {
                return rows.map(x => {
                    const rule : RuleObject = {
                        name: x.name!,
                        hash: x.hash!,
                        target: x.target!,
                        script: x.script!
                    }
                    return rule;
                });
            });
    }

    private _postProcess(executionContext: ExecutionContext, rulesDict: Record<string, RuleObject>)
    {
        return Promise.resolve()
            .then(() => this._saveRuleData(executionContext, rulesDict))
    }
    
    private _saveRuleData(executionContext : ExecutionContext, rulesDict: Record<string, RuleObject>)
    {
        return this._context.database.driver.executeInTransaction(() => {
            return Promise.resolve()
                .then(() => this._syncRuleStatuses(executionContext, rulesDict))
                .then(() => this._syncRuleItems(executionContext))
                .then(() => this._syncRuleLogs(executionContext))
                .then(() => this._syncMarkerItems(executionContext));
        });
    }

    private _syncRuleStatuses(executionContext : ExecutionContext, rulesDict: Record<string, RuleObject>)
    {
        this.logger.info('[_syncRuleStatuses] Begin');

        const rules = _.values(executionContext.rules);
        const ruleStatuses = 
            _.map(rules, x => ({
                rule_name: x.name,
                hash: rulesDict[x.name].hash,
                date: new Date(),
                error_count: x.error_count,
                item_count: x.items.length
            }))

        this.logger.debug('[_syncRuleStatuses] Rows: ', ruleStatuses);
        return this._ruleStatusesSynchronizer.execute(ruleStatuses);
    }

    private _syncRuleItems(executionContext : ExecutionContext)
    {
        this.logger.info('[_syncRuleItems] Begin');

        const ruleItems : any[] = [];

        for(const rule of _.values(executionContext.rules))
        {
            for(const item of rule.items)
            {
                ruleItems.push({
                    rule_name: rule.name,
                    dn: item.dn,
                    errors: item.errors,
                    warnings: item.warnings,
                    markers: item.markers
                })
            }
        }

        this.logger.debug('[_syncRuleItems] Rows: ', ruleItems);
        return this._ruleItemsSynchronizer.execute(ruleItems);
    }

    private _syncRuleLogs(executionContext : ExecutionContext)
    {
        this.logger.info('[_syncRuleLogs] Begin');

        const ruleLogs : any[] = [];

        for(const rule of _.values(executionContext.rules))
        {
            for(const log of rule.logs)
            {
                ruleLogs.push({
                    rule_name: rule.name,
                    kind: log.kind,
                    msg: log.msg
                })
            }
        }

        this.logger.debug('[_syncRuleLogs] Rows: ', ruleLogs);
        return this._ruleLogsSynchronizer.execute(ruleLogs);
    }

    private _syncMarkerItems(executionContext : ExecutionContext)
    {
        this.logger.info('[_syncRuleItems] Begin');

        const markerItems : any[] = [];

        for(const marker of _.values(executionContext.markers))
        {
            for(const dn of marker.items)
            {
                markerItems.push({
                    marker_name: marker.name,
                    dn: dn
                })
            }
        }

        this.logger.debug('[_syncRuleItems] Row: ', markerItems);
        return this._markerItemsSynchronizer.execute(markerItems);
    }
    
}
