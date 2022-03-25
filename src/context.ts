import _ from 'the-lodash';
import { ILogger } from 'the-logger';
import { Promise } from 'the-promise';

import { Backend } from '@kubevious/helper-backend'

import { FacadeRegistry } from './facade/registry';
import { SearchEngine } from './search/engine';
import { AutocompleteBuilder } from './search/autocomplete-builder';
import { Database } from './db';
// import { HistoryProcessor } from './history/processor';
// import { HistoryCleanupProcessor } from './history/history-cleanup-processor';
import { Registry } from './registry/registry';
import { Collector } from './collector/collector';
import { DebugObjectLogger } from './utils/debug-object-logger';
// import { MarkerAccessor } from './rule/marker-accessor';
// import { MarkerCache } from './rule/marker-cache';
// import { RuleAccessor } from './rule/rule-accessor';
// import { RuleCache } from './rule/rule-cache';
// import { RuleEngine } from './rule/rule-engine';
import { SnapshotProcessor } from './snapshot-processor';
import { WorldviousClient } from '@kubevious/worldvious-client';

import { WebServer } from './server';

// import { SnapshotReader as HistorySnapshotReader } from '@kubevious/helpers/dist/history/snapshot-reader';
import { SeriesResampler } from '@kubevious/helpers/dist/history/series-resampler';

import { ParserLoader } from '@kubevious/helper-logic-processor';
import { Executor } from './app/executor/executor'

import { SnapshotPersistor } from './app/persistor';


import VERSION from './version'

export class Context
{
    private _backend : Backend;
    private _logger: any; //  ILogger;
    /* Both of the 'DumpWriter' class (inside the-logger and worldvious-client/node_modules/the-logger)
    should have public _writer and _indent properties to be able to uncomment */
    private _worldvious : WorldviousClient;

    private _server: WebServer;

    private _dataStore: Database;
    private _searchEngine: SearchEngine;
    // private _historyProcessor: HistoryProcessor;

    private _collector: Collector;
    private _registry: Registry;
    private _autocompleteBuilder: AutocompleteBuilder;

    private _facadeRegistry: FacadeRegistry;

    private _debugObjectLogger: DebugObjectLogger;

    private _executor : Executor;

    // private _markerAccessor: MarkerAccessor;
    // private _markerCache: MarkerCache;
    // private _ruleAccessor: RuleAccessor;
    // private _ruleCache: RuleCache;
    // private _ruleEngine: RuleEngine;

    // private _historySnapshotReader: HistorySnapshotReader;

    private _snapshotProcessor: SnapshotProcessor;
    private _snapshotPersistor : SnapshotPersistor;

    // private _historyCleanupProcessor: HistoryCleanupProcessor;

    private _seriesResamplerHelper: SeriesResampler;

    private _parserLoader : ParserLoader;

    constructor(backend : Backend)
    {
        this._backend = backend;
        this._logger = backend.logger.sublogger('Context');

        this._logger.info("Version: %s", VERSION);

        this._worldvious = new WorldviousClient(this.logger, 'collector', VERSION);

        this._parserLoader = new ParserLoader(this.logger);

        this._dataStore = new Database(this._logger, this);
        this._searchEngine = new SearchEngine(this);
        // this._historyProcessor = new HistoryProcessor(this);
        this._collector = new Collector(this);
        this._registry = new Registry(this);
        this._autocompleteBuilder = new AutocompleteBuilder(this);

        this._facadeRegistry = new FacadeRegistry(this);
        this._executor = new Executor(this);

        this._debugObjectLogger = new DebugObjectLogger(this);

        // this._markerAccessor = new MarkerAccessor(this, this.database.dataStore);
        // this._markerCache = new MarkerCache(this);
        // this._ruleAccessor = new RuleAccessor(this, this.database.dataStore);
        // this._ruleCache = new RuleCache(this);
        // this._ruleEngine = new RuleEngine(this, this.database.dataStore);

        // this._historySnapshotReader = new HistorySnapshotReader(this.logger, this._dataStore.driver);

        this._snapshotProcessor = new SnapshotProcessor(this);
        this._snapshotPersistor = new SnapshotPersistor(this);

        // this._historyCleanupProcessor = new HistoryCleanupProcessor(this);

        this._seriesResamplerHelper = new SeriesResampler(200)
            .column("changes", _.max)
            .column("error", _.mean)
            .column("warn", _.mean)
            ;

        this._server = new WebServer(this);

        backend.registerErrorHandler((reason) => {
            return this.worldvious.acceptError(reason);
        });

        backend.stage("setup-worldvious", () => this._worldvious.init());

        backend.stage("setup-metrics-tracker", () => this._setupMetricsTracker());

        backend.stage("setup-db", () => this._dataStore.init());

        backend.stage("setup-parser-loader", () => this._parserLoader.init());

        backend.stage("setup-server", () => this._server.run());
    }

    get backend() {
        return this._backend;
    }

    get logger() {
        return this._logger;
    }

    get tracker() {
        return this.backend.tracker;
    }

    get mysqlDriver() {
        return this.database.driver;
    }

    get database() {
        return this._dataStore;
    }

    get dataStore() {
        return this._dataStore;
    }

    get executor() : Executor {
        return this._executor;
    }
    
    get facadeRegistry() {
        return this._facadeRegistry;
    }

    get searchEngine() {
        return this._searchEngine;
    }

    // get historyProcessor() {
    //     return this._historyProcessor;
    // }

    get collector() {
        return this._collector;
    }

    get registry() {
        return this._registry;
    }

    get debugObjectLogger() {
        return this._debugObjectLogger;
    }

    // get markerAccessor() {
    //     return this._markerAccessor;
    // }

    // get markerCache() {
    //     return this._markerCache;
    // }

    // get ruleAccessor() {
    //     return this._ruleAccessor;
    // }

    // get ruleCache() {
    //     return this._ruleCache;
    // }

    // get ruleEngine() {
    //     return this._ruleEngine;
    // }

    // get historySnapshotReader() {
    //     return this._historySnapshotReader;
    // }

    get snapshotProcessor() {
        return this._snapshotProcessor;
    }

    get snapshotPersistor() {
        return this._snapshotPersistor;
    }

    // get historyCleanupProcessor() {
    //     return this._historyCleanupProcessor;
    // }

    get worldvious() {
        return this._worldvious;
    }

    get seriesResamplerHelper() {
        return this._seriesResamplerHelper;
    }

    get autocompleteBuilder() {
        return this._autocompleteBuilder;
    }

    get parserLoader() {
        return this._parserLoader;
    }

    private _setupMetricsTracker()
    {
        this.tracker.registerListener(extractedData => {
            this._worldvious.acceptMetrics(extractedData);
        })
    }

}
