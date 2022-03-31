import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { RegistryState, RegistryBundleState } from '@kubevious/state-registry';

import { Context } from '../context';

export class Registry
{
    private _context : Context;
    private _logger : ILogger;

    private _currentState : RegistryBundleState;

    constructor(context : Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger("Registry");

        this._currentState = new RegistryState({ date: new Date(), items: []}).buildBundle();
    }

    get logger() {
        return this._logger;
    }

    getCurrentState() : RegistryBundleState
    {
        return this._currentState;
    }

    accept(bundle : RegistryBundleState)
    {
        this._currentState = bundle;
    }

}