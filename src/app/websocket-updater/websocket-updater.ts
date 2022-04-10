import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { Context } from '../../context';
import { HttpClient } from '@kubevious/http-client';

export class WebSocketUpdater
{
    private _logger : ILogger;
    private _context : Context;

    private _backendClient : HttpClient;

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('SnapshotProcessor');

        const baseUrl = process.env.BACKEND_BASE_URL;
        if (!baseUrl) {
            throw new Error("ENV BACKEND_BASE_URL not set.");
        }
        this._backendClient = new HttpClient(baseUrl);
    }

    get logger() {
        return this._logger;
    }

    notifyNewSnapshot()
    {
        const body = {
            items: [
                { target: WebSocketKind.latest_snapshot_id },
                { target: WebSocketKind.rules_statuses },
                { target: WebSocketKind.rule_result },
                { target: WebSocketKind.markers_statuses },
                { target: WebSocketKind.marker_result },
            ]
        }
        return this._notifySocket(body);
    }

    notifyReporter()
    {
        const body = {
            items: [
                { target: WebSocketKind.cluster_reporting_status },
            ]
        }
        return this._notifySocket(body);
    }

    private _notifySocket(body: any)
    {
        return this._backendClient.post('/api/internal/socket/report', {}, body);
    }
}

export enum WebSocketKind
{
    node = 'node',
    children = 'children',
    props = 'props',
    alerts = 'alerts',

    latest_snapshot_id = 'latest_snapshot_id',

    rules_list = 'rules-list',
    rules_statuses = 'rules-statuses',
    rule_result = 'rule-result',
    markers_list = 'markers-list',
    markers_statuses = 'markers-statuses',
    marker_result = 'marker-result',

    cluster_reporting_status = 'cluster_reporting_status'
}