import _ from 'the-lodash';
import { Context } from '../context';
import { Router } from '@kubevious/helper-backend'
import Joi from 'joi';
import { ILogger } from 'the-logger';

import { RequestReportSnapshot, RequestReportSnapshotItems, RequestActivateSnapshot, RequestReportConfig } from '@kubevious/agent-middleware'

export default function (router: Router, context: Context, logger: ILogger) {

    router.url('/api/v1/collect');
    
    router.post<{}, RequestReportSnapshot>('/snapshot', function (req, res) {

        const data = req.body;

        const date = new Date(data.date);
        const parserVersion = data.version;
        const baseSnapshotId = data.snapshot_id;

        logger.info("New snapshot reporting started. Date %s. ParserVersion: %s.", date.toISOString(), parserVersion);

        return context.collector.newSnapshot(date, parserVersion, baseSnapshotId);
    })
    .bodySchema(
        Joi.object({
            date: Joi.string().isoDate().required(),
            version: Joi.string().optional(),
            snapshot_id: Joi.string().optional(),
        })
    );

    router.post<{}, RequestReportSnapshotItems>('/snapshot/items', function (req, res) {
        const data = req.body;

        return context.collector.acceptSnapshotItems(data.snapshot_id, data.items);
    })
    .bodySchema(
        Joi.object({
            snapshot_id: Joi.string().required(),
            items: Joi.array().required().items(Joi.object())
        })
    );

    router.post<{}, RequestActivateSnapshot>('/snapshot/activate', function (req, res) {

        return context.collector.activateSnapshot(req.body.snapshot_id);
    })
    .bodySchema(
        Joi.object({
            snapshot_id: Joi.string().required(),
            items: Joi.array().items(
                Joi.object({
                    idHash: Joi.string().required(),
                    present: Joi.boolean().required(),
                    configHash: Joi.string().optional(),
                })
            )
        })
    );

    router.post<{}, RequestReportConfig>('/config', (req, res) => {

        const data = req.body;

        return context.collector.storeConfig(data.hash, data.config);

    })
    .bodySchema(
        Joi.object({
            hash: Joi.string().required(),
            config: Joi.object().required()
        })
    );



}
