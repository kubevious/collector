import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import * as fs from 'fs';
import * as Path from 'path';

import { RegistryState, RegistryBundleState } from '@kubevious/state-registry';

import { ProcessorBuilder, Handler as ProcessorHandler, HandlerArgs } from './builder';
import { ExecutionContext as RuleEngineExecutionContext, RuleObject } from '@kubevious/helper-rule-engine';

import { Context } from '../context';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

interface ProcessorEntry
{
    name: string;
    order: number;
    handler: ProcessorHandler;
}

export class SnapshotProcessor
{
    private _logger : ILogger;
    private _context : Context;

    private _processors : ProcessorEntry[] = [];

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('SnapshotProcessor');

        this._extractProcessors();
    }

    get logger() {
        return this._logger;
    }

    private _extractProcessors()
    {
        const location = 'processors';
        const processorsDir = Path.join(__dirname, location);
        this.logger.info('[_extractProcessors] from %s', processorsDir);
        let files = fs.readdirSync(processorsDir);
        files = _.filter(files, x => x.endsWith('.d.ts'));

        for(const fileName of files)
        {
            const moduleName = fileName.replace('.d.ts', '');
            const modulePath = location + '/' + moduleName;
            this._logger.info("Loading processor %s from %s...", moduleName, modulePath);

            const processorModule = require('./' + modulePath);
            const processorBuilder = <ProcessorBuilder> processorModule.default;
            const processorInfo = processorBuilder._export();

            if (!processorInfo.isDisabled)
            {
                this._processors.push({
                    name: modulePath,
                    order: processorInfo.order,
                    handler: processorInfo.handler!
                });
            }
        }
        this._processors = _.orderBy(this._processors, x => x.order);

        for(const processor of this._processors)
        {
            this._logger.info("[_extractProcessors] HANDLER: %s :: %s", 
                processor.order, 
                processor.name)
        }
    }

    process(registryState : RegistryState, rules: RuleObject[], tracker: ProcessingTrackerScoper)
    {
        const ruleEngineResult: RuleEngineExecutionContext = {
            rules: {},
            markers: {}
        }

        let bundle : RegistryBundleState | null = null;
        return Promise.resolve()
            .then(() => this._runProcessors(registryState, rules, ruleEngineResult, tracker))
            .then(() => {
                return tracker.scope("buildBundle", () => {
                    bundle = registryState!.buildBundle();
                });
            })
            .then(() => {
              
                return {
                    bundle: bundle!,
                    ruleEngineResult: ruleEngineResult
                }
            })
    }

    private _runProcessors(registryState: RegistryState,
                           rules: RuleObject[],
                           ruleEngineResult: RuleEngineExecutionContext,
                           tracker : ProcessingTrackerScoper)
    {
        return tracker.scope("handlers", (procTracker) => {
            return Promise.serial(this._processors, processor => {
                return procTracker.scope(processor.name, (innerTracker) => {

                    const params : HandlerArgs = {
                        logger: this.logger,
                        context: this._context,
                        state: registryState,
                        rules: rules,
                        ruleEngineResult: ruleEngineResult,
                        tracker: innerTracker
                    };
                    
                    return processor.handler(params);
                })
            })
        });
    }

}
