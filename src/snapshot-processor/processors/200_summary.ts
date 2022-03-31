import _ from 'the-lodash';
import { RegistryStateNode } from '@kubevious/state-registry';
import { Alert, AlertCounter, SnapshotConfigKind, SnapshotNodeConfig, SnapshotPropsConfig } from '@kubevious/state-registry'
import { Processor } from '../builder'
import { NodeKind, PropsId, PropsKind } from '@kubevious/entity-meta';

export default Processor()
    .order(200)
    .handler(({logger, state, tracker, context }) => {

        state.addNewItem({
            dn: 'summary',
            kind: 'summary',
            config_kind: SnapshotConfigKind.node,
            config: <SnapshotNodeConfig> {
                kind: NodeKind.summary,
                rn: 'summary',
                name: 'summary'
            }
        });

        state.addNewItem({
            dn: 'summary',
            kind: 'summary',
            config_kind: SnapshotConfigKind.props,
            config: <SnapshotPropsConfig> {
                kind: PropsKind.counters,
                id: PropsId.appCounters,
                title: 'Configuration Summary',
                order: 10,
                config: [{
                    title: 'Namespaces',
                    value: state.countScopeByKind('root/logic', NodeKind.ns)
                }, {
                    title: 'Applications',
                    value: state.countScopeByKind('root/logic', NodeKind.app)
                }, {
                    title: 'Pods',
                    value: state.countScopeByKind('root/logic', NodeKind.pod)
                }]
            }
        });

        state.addNewItem({
            dn: 'summary',
            kind: 'summary',
            config_kind: SnapshotConfigKind.props,
            config: <SnapshotPropsConfig> {
                kind: PropsKind.counters,
                id: PropsId.infraCounters,
                title: 'Infrastructure Summary',
                order: 11,
                config: [{
                    title: 'Nodes',
                    value: state.countScopeByKind('root/infra/nodes', NodeKind.node)
                }, 
                getVolumeCount(), 
                getClusterCPU(), 
                getClusterRAM(),
                getClusterStorage()
                ]
            }
        });

        state.addNewItem({
            dn: 'summary',
            kind: 'summary',
            config_kind: SnapshotConfigKind.props,
            config: <SnapshotPropsConfig> {
                kind: PropsKind.objectList,
                id: PropsId.topIssueNamespaces,
                title: 'Top Namespaces with Issues',
                order: 12,
                config: getTopNamespacesWithIssues()
            }
        });

        state.addNewItem({
            dn: 'summary',
            kind: 'summary',
            config_kind: SnapshotConfigKind.props,
            config: <SnapshotPropsConfig> {
                kind: PropsKind.alertTargetList,
                id: PropsId.topIssues,
                title: 'Top Issues',
                order: 13,
                config: getTopIssues()
            }
        });

        /***********/

        function getTopNamespacesWithIssues()
        {
            const namespaces = state.scopeByKind('root/logic', NodeKind.ns);

            const namespaceInfos : { dn: string, counters: NamespaceAlertCounters }[] = [];

            for(const ns of _.values(namespaces))
            {
                const counters: NamespaceAlertCounters = {
                    totalIssues: 0,
                    alertCount: {
                        error: 0,
                        warn: 0
                    }
                }
                
                extractNamespaceAlerts(ns, counters);

                counters.totalIssues = counters.alertCount.error * 2 + counters.alertCount.warn;

                namespaceInfos.push({
                    dn: ns.dn,
                    counters: counters
                });
            }

            const orderedNamespaces = _.orderBy(namespaceInfos, x => x.counters.totalIssues, 'desc');

            const topNamespaces = _.take(orderedNamespaces, 3);

            return topNamespaces.map(x => {
                return {
                    dn: x.dn,
                    alertCount: x.counters.alertCount
                }
            });
        }

        function getTopIssues()
        {
            const alertDict: Record<string, { alert: Alert, targets: string[] } > = {};

            for(const node of state.scopeFlat('root'))
            {
                const alerts = state.getAlerts(node.dn);
                for(const alert of alerts)
                {
                    const alertKey = getAlertKey(alert);
                    if (!alertDict[alertKey]) {
                        alertDict[alertKey] = {
                            alert: alert,
                            targets: []
                        };
                    } 
    
                    alertDict[alertKey].targets.push(node.dn);
                }
            }

            const orderedAlerts = _.orderBy(_.values(alertDict), x => x.targets.length, 'desc');
            
            const topAlerts = _.take(orderedAlerts, 3);
            return topAlerts;
        }

        function extractNamespaceAlerts(node: RegistryStateNode, counters: NamespaceAlertCounters)
        {
            for(const alert of node.selfAlerts)
            {
                (<Record<string, number>> <any> counters.alertCount) [alert.severity] += 1;
            }

            for(const childDn of state.getChildrenDns(node.dn))
            {
                const childNode = state.getNode(childDn)!;
                extractNamespaceAlerts(childNode, counters);
            }
        }

        function getAlertKey(alert: Alert)
        {
            return [alert.severity, alert.msg, _.stableStringify(alert.source)].join('-');
        }

        function getClusterCPU()
        {
            let value = getInfraCapacity('cpu allocatable') as PropertyValueWithUnit;
            if (!value) {
                value = {
                    value: 0,
                    unit: 'cores'
                }
            }

            return {
                title: 'Cluster CPU',
                value: value.value,
                unit: value.unit
            }
        }

        function getClusterRAM()
        {
            let value = getInfraCapacity('memory allocatable') as PropertyValueWithUnit;
            if (!value) {
                value = {
                    value: 0,
                    unit: 'bytes'
                }
            }

            return {
                title: 'Cluster RAM',
                value: value.value,
                unit: value.unit
            }
        }

        function getVolumeCount() 
        {
            let value = getStorageCapacity('Volume Count');
            if (!value) {
                value = 0;
            }

            return {
                title: 'Volumes',
                value: value
            } 
        }

        function getClusterStorage() 
        {
            let value = getStorageCapacity('Capacity') as PropertyValueWithUnit;
            if (!value) {
                value = {
                    value: 0,
                    unit: 'bytes'
                }
            }

            return {
                title: 'Cluster Storage',
                value: value.value,
                unit: value.unit
            } 
        }

        function getInfraCapacity(key: string)
        {
            const item = state.findByDn('root/infra/nodes');
            if (!item) {
                return '?';
            }
            const config = item.getProperties('cluster-resources');
            if (!config) {
                return '?';
            }
            const value = config.config[key];
            return value;
        }

        function getStorageCapacity(key: string)
        {
            const item = state.findByDn('root/infra/storage');
            if (!item) {
                return 0;
            }
            const config = item.getProperties('properties');
            if (!config) {
                return 0;
            }
            const value = config.config[key];
            return value;
        }

    })


interface NamespaceAlertCounters
{
    totalIssues: number;
    alertCount: AlertCounter;
}

interface PropertyValueWithUnit
{
    value: number,
    unit: string
}