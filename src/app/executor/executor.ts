import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { Promise } from 'the-promise';

import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

import { Context } from '../../context'

import { ExecutorTarget, ExecutorTaskTarget } from './types';
import { ExecutorTask } from './executor-task';
import * as BufferUtils from '@kubevious/helpers/dist/buffer-utils';

export class Executor
{
    private _logger : ILogger;
    private _context : Context;

    constructor(context : Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('Executor');
    }

    get logger() {
        return this._logger;
    }

    process(target: ExecutorTarget) : Promise<any>
    {
        const myTarget = this._makeTaskTarget(target);

        return this._context.tracker.scope("executor", (innerTracker) => {
            const task = new ExecutorTask(this._logger, this._context, myTarget);
            return task.execute(innerTracker);
        })
        .then(() => {
            this._markComplete(target);
        })
        .catch((error) => {
            this._logger.error("[Executor] ERROR: ", error);

            this._markComplete(target);
        })
        ;
    }
    
    private _makeTaskTarget(target: ExecutorTarget) : ExecutorTaskTarget
    {
        const myTarget : ExecutorTaskTarget = {
            registry: target.registry,
            snapshotId: BufferUtils.fromStr(target.snapshotId),
            date: target.date
        }
        return myTarget;
    }

    private _markComplete(target: ExecutorTarget)
    {
        this._context.collector.completeSnapshotProcessing(target.snapshotId);
    }

}