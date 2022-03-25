import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

import { Context } from '../../context'

import { SnapshotPersistorTask } from './task';
import { SnapshotPersistorTarget } from './types';


export class SnapshotPersistor
{
    private _logger : ILogger;
    private _context : Context;

    constructor(context : Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('SnapshotPersistor');
    }

    get logger() {
        return this._logger;
    }

    persist(target: SnapshotPersistorTarget, tracker: ProcessingTrackerScoper) : Promise<any>
    {
        return tracker.scope("persist-snapshot", (innerTracker) => {
            const task = new SnapshotPersistorTask(this._logger, this._context, target);
            return task.execute(innerTracker);
        });
    }
}