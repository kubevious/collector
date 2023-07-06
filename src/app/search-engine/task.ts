import { ILogger } from 'the-logger';
import _ from 'the-lodash';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';

import { Context } from '../../context'
import { SearchEnginePersistorTarget } from './types';
import { RedisClient } from '@kubevious/helper-redis';
import { RedisSearchNameFetcher } from '@kubevious/data-models';
import { HashUtils } from '@kubevious/data-models';
import { RegistryBundleNode } from '@kubevious/state-registry';
import { MyPromise } from 'the-promise';

const SKIPPED_ANNOTATIONS : Record<string, boolean> = {
    'kubectl.kubernetes.io/last-applied-configuration': true
}

const RECREATE_INDEXES : boolean = false;

export class SearchEnginePersistorTask
{
    private _logger : ILogger;
    private _context : Context;
    private _target: SearchEnginePersistorTarget;
    private _redis : RedisClient;

    private _nameFetcher : RedisSearchNameFetcher;

    private _searchItems : Record<string, SearchItem> = {}
    private _labelItems : Record<string, SearchItem> = {}
    private _annoItems : Record<string, SearchItem> = {}

    constructor(logger: ILogger, context : Context, target: SearchEnginePersistorTarget)
    {
        this._context = context;
        this._logger = logger;

        this._target = target;

        this._redis = context.redis;

        this._nameFetcher = new RedisSearchNameFetcher();
    }

    get logger() {
        return this._logger;
    }

    execute(tracker: ProcessingTrackerScoper)
    {
        return Promise.resolve()
            .then(() => {
                return tracker.scope("construct", () => {

                    const items = this._target.stateBundle.nodeItems.filter(x => _.startsWith(x.dn, 'root/'));

                    this._constructNodeTarget(items);
                    this._constructLabelTarget(items);
                    this._constructAnnotationTarget(items);
                });
            })
            .then(() => {
                return tracker.scope("node-data", (innerTracker) => {
                    return this._setupNodeData(innerTracker);
                });
            })
            .then(() => {
                return tracker.scope("label-data", (innerTracker) => {
                    return this._setupLabelData(innerTracker);
                });
            })
            .then(() => {
                return tracker.scope("anno-data", (innerTracker) => {
                    return this._setupAnnoData(innerTracker);
                });
            })
            // .then(() => MyPromise.delay(10000))
            // .then(()=> {
            //     throw new Error("ZZZZ")
            // })
    }

    private _setupNodeData(tracker: ProcessingTrackerScoper) {
        return Promise.resolve()
            .then(() => {
                return tracker.scope("fetch", () => {
                    return this._fetchCurrentKeyHashes(this._nameFetcher.nodeKeyPrefix);
                });
            })
            .then((keyHashes) => {
                return tracker.scope("delta", () => {
                    const delta = this._produceDelta(keyHashes, this._searchItems);
                    this.logger.info("[_setupNodeData] target count: %s", _.keys(this._searchItems).length);
                    this.logger.info("[_setupNodeData] delta updated count: %s, deleted count: %s.", delta.items.length, delta.toDelete.length);
                    // this.logger.info("delta: ", delta);
                    return delta;
                });
            })
            .then(delta => {
                return tracker.scope("apply", () => {
                    return this._applyChanges(delta);
                });
            })
            .then(() => this._createNodeIndex())
    }

    private _setupLabelData(tracker: ProcessingTrackerScoper) {
        return Promise.resolve()
            .then(() => {
                return tracker.scope("fetch", () => {
                    return this._fetchCurrentKeyHashes(this._nameFetcher.labelKeyPrefix);
                });
            })
            .then((keyHashes) => {
                return tracker.scope("delta", () => {
                    const delta = this._produceDelta(keyHashes, this._labelItems);
                    this.logger.info("[_setupLabelData] target count: %s", _.keys(this._labelItems).length);
                    this.logger.info("[_setupLabelData] delta updated count: %s, deleted count: %s.", delta.items.length, delta.toDelete.length);
                    // this.logger.info("delta: ", delta);
                    return delta;
                });
            })
            .then(delta => {
                return tracker.scope("apply", () => {
                    return this._applyChanges(delta);
                });
            })
            .then(() => this._createLabelIndex())
    }

    private _setupAnnoData(tracker: ProcessingTrackerScoper) {
        return Promise.resolve()
            .then(() => {
                return tracker.scope("fetch", () => {
                    return this._fetchCurrentKeyHashes(this._nameFetcher.annoKeyPrefix);
                });
            })
            .then((keyHashes) => {
                return tracker.scope("delta", () => {
                    const delta = this._produceDelta(keyHashes, this._annoItems);
                    this.logger.info("[_setupAnnoData] target count: %s", _.keys(this._annoItems).length);
                    this.logger.info("[_setupAnnoData] updated count: %s, deleted count: %s.", delta.items.length, delta.toDelete.length);
                    return delta;
                });
            })
            .then(delta => {
                return tracker.scope("apply", () => {
                    return this._applyChanges(delta);
                });
            })
            .then(() => this._createAnnotationIndex())
    }


    private _fetchCurrentKeyHashes(keyPrefix: string)
    {
        const keyHashes : Record<string, string> = {}
        return this._redis.filterValues(`${keyPrefix}:*`)
            .then(keys => {
                return MyPromise.execute(keys, (x) => {
                    return this._redis.hashSet(x).getField('hash')
                        .then(hash => {
                            if (hash) {
                                keyHashes[x] = hash;
                            }
                        })
                }, {
                    concurrency: 100
                })
            })
            .then(() => {
                return keyHashes;
            })
    }

    private _produceDelta(currentKeyHashes : Record<string, string>, targetSearchItems : Record<string, SearchItem>) : Delta
    {
        const keysToDelete : string[] = [];
        const updatedSearchItems : SearchItem[] = [];

        for(const searchItem of _.values(targetSearchItems))
        {
            const currentHash = currentKeyHashes[searchItem.key];
            if (!currentHash)
            {
                updatedSearchItems.push(searchItem);
            }
            else
            {
                if (searchItem.hash !== currentHash)
                {
                    updatedSearchItems.push(searchItem);
                }
            }
        }

        for(const key of _.keys(currentKeyHashes))
        {
            if (!targetSearchItems[key]) {
                keysToDelete.push(key)
            }
        }

        return {
            items: updatedSearchItems,
            toDelete: keysToDelete
        }

    }

    private _constructNodeTarget(nodeItems: RegistryBundleNode[])
    {
        for(const node of nodeItems)
        {
            const config : Record<string, any> = {
                text: node.dn,
                dn: node.dn,
                kind: node.kind,
                error: node.config.alertCount.error,
                warn: node.config.alertCount.warn,
                self_error: node.config.selfAlertCount.error,
                self_warn: node.config.selfAlertCount.warn
            };

            config[`markers`] = _.keys(node.markers).join(',');

            const hash = HashUtils.calculateObjectHashStr(config);
            
            config['hash'] = hash;

            const item: SearchItem = {
                key: [
                    this._nameFetcher.nodeKeyPrefix,
                    node.dn
                ].join(':'),
                hash: hash,
                config: config
            }
            this._searchItems[item.key] = item;
        }
    }

    private _constructLabelTarget(nodeItems: RegistryBundleNode[])
    {
        for(const node of nodeItems)
        {
            for (const key of _.keys(node.labels))
            {
                const value = node.labels[key];

                const config : Record<string, any> = {
                    dn: node.dn,
                    key: key,
                    value: value
                };

                const hash = HashUtils.calculateObjectHashStr(config);
                config['hash'] = hash;

                const item: SearchItem = {
                    key: [
                        this._nameFetcher.labelKeyPrefix,
                        node.dn,
                        key
                    ].join(':'),
                    hash: hash,
                    config: config
                }

                this._labelItems[item.key] = item;
            }
        }
    }

    private _constructAnnotationTarget(nodeItems: RegistryBundleNode[])
    {
        for(const node of nodeItems)
        {
            for (const key of _.keys(node.annotations))
            {
                if (SKIPPED_ANNOTATIONS[key]) {
                    continue;
                }
                const value = node.annotations[key];

                const config : Record<string, any> = {
                    dn: node.dn,
                    key: key,
                    value: value
                };

                const hash = HashUtils.calculateObjectHashStr(config);
                config['hash'] = hash;

                const item: SearchItem = {
                    key: [
                        this._nameFetcher.annoKeyPrefix,
                        node.dn,
                        key
                    ].join(':'),
                    hash: hash,
                    config: config
                }

                this._annoItems[item.key] = item;
            }
        }
    }

    private _applyChanges(delta: Delta)
    {
        return Promise.resolve()
            .then(() => {
                return MyPromise.execute(delta.toDelete, x => {
                    return this._redis.hashSet(x).delete();
                }, {
                    concurrency: 100
                });
            })
            .then(() => {
                return MyPromise.execute(delta.items, x => {
                    const client = this._redis.hashSet(x.key);
                    return client.delete()
                        .then(() => client.set(x.config));
                }, {
                    concurrency: 100
                });
            })
    }

    private _createNodeIndex()
    {
        const indexClient = this._redis.redisearch.index(this._nameFetcher.nodeSearchIndex);

        return Promise.resolve()
            .then(() => {
                if (!RECREATE_INDEXES) {
                    return;
                }
                return indexClient
                    .delete();
            })
            .then(() => {
                return indexClient
                    .create({
                        count: 1,
                        prefix: `${this._nameFetcher.nodeKeyPrefix}:`
                    }, [
                        { name: 'text' },
                        { name: 'kind', type: 'TAG' },
                        { name: 'error', type: 'NUMERIC' },
                        { name: 'warn', type: 'NUMERIC' },
                        { name: 'markers', type: 'TAG' },
                    ])
            })
    }

    private _createLabelIndex()
    {
        const indexClient = this._redis.redisearch.index(this._nameFetcher.labelSearchIndex);

        return Promise.resolve()
            .then(() => {
                if (!RECREATE_INDEXES) {
                    return;
                }
                return indexClient
                    .delete();
            })
            .then(() => {
                return indexClient
                    .create({
                        count: 1,
                        prefix: `${this._nameFetcher.labelKeyPrefix}:`
                    }, [
                        { name: 'key' },
                        { name: 'value' },
                    ])
            })
    }

    private _createAnnotationIndex()
    {
        const indexClient = this._redis.redisearch.index(this._nameFetcher.annoSearchIndex);

        return Promise.resolve()
            .then(() => {
                if (!RECREATE_INDEXES) {
                    return;
                }
                return indexClient
                    .delete();
            })
            .then(() => {
                return indexClient
                    .create({
                        count: 1,
                        prefix: `${this._nameFetcher.annoKeyPrefix}:`
                    }, [
                        { name: 'key' },
                        { name: 'value' },
                    ])
            })
    }
}

interface SearchItem {
    key: string,   
    hash: string,   
    config: Record<string, any>
}

interface Delta {
    items: SearchItem[],
    toDelete: string[]
}