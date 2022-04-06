import _ from 'the-lodash';
import { Context } from '../context';
import { Router } from '@kubevious/helper-backend'
import VERSION from '../version';
import { BackendVersionResponse, BackendMetricsResponse } from '@kubevious/ui-middleware'

export default function (router: Router, context: Context) {
    router.url('/');

    router.get('/', (req, res) => {
        return {};
    });

    router.get('/api/v1/version', (req, res) => {
        const result : BackendVersionResponse = {
            version: VERSION
        };
        return result;
    });

    router.get('/api/v1/metrics', (req, res) => {
        const metrics = context.backendMetrics.extractMetrics();

        const result : BackendMetricsResponse = {
            metrics: metrics
        };

        return result;
    });
}