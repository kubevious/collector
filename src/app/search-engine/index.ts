import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { ProcessingTrackerScoper } from '@kubevious/helper-backend';
import { Context } from '../../context'

import { SearchEnginePersistorTask } from './task';
import { SearchEnginePersistorTarget } from './types';


export class SearchEnginePersistor
{
    private _logger : ILogger;
    private _context : Context;

    constructor(context : Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('SearchEnginePersistor');
    }

    get logger() {
        return this._logger;
    }

    persist(target: SearchEnginePersistorTarget, tracker: ProcessingTrackerScoper) : Promise<any>
    {
        const task = new SearchEnginePersistorTask(this._logger, this._context, target);
        return task.execute(tracker);
    }
}