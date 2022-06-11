import _ from 'the-lodash';
import { Promise, Resolvable } from 'the-promise';
import { ILogger } from 'the-logger' ;

import * as fs from 'fs';
import * as Path from 'path';

import { DataStore, DataStoreTableAccessor, MySqlDriver, MySqlStatement } from '@kubevious/easy-data-store';

import { Context } from '../context' ;

import { MigratorArgs, MigratorBuilder, MigratorInfo, SqlBuilder } from './migration';

import { ConfigAccessors, prepareConfig } from '@kubevious/data-models/dist/models/config'
import { SnapshotsAccessors, prepareSnapshots } from '@kubevious/data-models/dist/models/snapshots'
import { RuleEngineAccessors, prepareRuleEngine } from '@kubevious/data-models/dist/models/rule_engine'
import { ValidationAccessors, prepareValidation } from '@kubevious/data-models/dist/models/validation'
import { LogicStoreAccessors, prepareLogicStore } from '@kubevious/data-models/dist/models/logic-store'

const TARGET_DB_VERSION : number = 11;

const DB_NAME = process.env.MYSQL_DB;

export class Database
{
    private _logger : ILogger;
    private _context : Context

    private _migrators : Record<string, MigratorInfo> = {};

    private _dataStore : DataStore;
    private _driver? : MySqlDriver;
    private _statements : Record<string, MySqlStatement> = {};

    private _config : ConfigAccessors;
    private _snapshots : SnapshotsAccessors;
    private _ruleEngine : RuleEngineAccessors;
    private _validation : ValidationAccessors;
    private _logicStore : LogicStoreAccessors;


    constructor(logger : ILogger, context : Context)
    {
        this._context = context;
        this._logger = logger.sublogger("DB");

        this._loadMigrators();

        this._dataStore = new DataStore(logger.sublogger("DataStore"), false);

        this._config = prepareConfig(this._dataStore);
        this._snapshots = prepareSnapshots(this._dataStore);
        this._ruleEngine = prepareRuleEngine(this._dataStore);
        this._validation = prepareValidation(this._dataStore);
        this._logicStore = prepareLogicStore(this._dataStore);
    }

    get logger() {
        return this._logger;
    }

    get dataStore() {
        return this._dataStore;
    }

    get driver() {
        return this._driver!;
    }

    get isConnected() {
        return this._dataStore.isConnected;
    }

    get config() {
        return this._config;
    }

    get snapshots() {
        return this._snapshots;
    }

    get ruleEngine() {
        return this._ruleEngine;
    }

    get validation() {
        return this._validation;
    }

    get logicStore() {
        return this._logicStore;
    }

    private _loadMigrators()
    {
        const location = 'migrators';
        const migratorsDir = Path.join(__dirname, location);

        let files = fs.readdirSync(migratorsDir);
        files = _.filter(files, x => x.endsWith('.d.ts'));

        for(const fileName of files)
        {
            const moduleName = fileName.replace('.d.ts', '');
            const modulePath = location + '/' + moduleName;
            this._logger.info("Loading migrator %s from %s...", moduleName, modulePath);

            const migratorModule = require('./' + modulePath);
            const migrationBuilder = <MigratorBuilder> migratorModule.default;
            const migrationInfo = migrationBuilder._export();
            this._logger.info("migrationInfo: ", migrationInfo);

            this._logger.info("Loaded migrator %s from %s", moduleName, modulePath);
            this._migrators[moduleName] = migrationInfo;
        }
    }

    onConnect(cb: () => Resolvable<any>)
    {
        return this._driver!.onConnect(cb);
    }

    table<TRow>(accessor: DataStoreTableAccessor<TRow>)
    {
        return this._dataStore.table(accessor);
    }

    executeInTransaction<T = any>(tables: (string | DataStoreTableAccessor<any>)[], cb: () => Resolvable<T>): Promise<any>
    {
        return this._dataStore.executeInTransaction(tables, cb);
    }

    init()
    {
        this._logger.info("[init]")

        return Promise.resolve()
            .then(() => this._dataStore.init())
            .then(() => {
                this._driver = this._dataStore.mysql!.databaseClients.find(x => x.name === DB_NAME)!.client;
                this._driver.onMigrate(this._onDbMigrate.bind(this));
            })
            .then(() => {
                this._logger.info("[init] post connect.")
            })
    }

    private _onDbMigrate()
    {
        this._logger.info("[_onDbMigrate] ...");
        return Promise.resolve()
            .then(() => this._processMigration())
            ;
    }

    private _processMigration()
    {
        this.logger.info("[_processMigration] ...");

        return this.driver.executeInTransaction(() => {
            return Promise.resolve()
                .then(() => this._getDbVersion())
                .then(version => {
                    this.logger.info("[_processMigration] VERSION: %s", version);
                    this.logger.info("[_processMigration] TARGET_DB_VERSION: %s", TARGET_DB_VERSION);
                    if (version == TARGET_DB_VERSION) {
                        return;
                    }
                    if (version > TARGET_DB_VERSION) {
                        this.logger.error("[_processMigration] You are running database version more recent then the binary. Results may be unpredictable.");
                        return;
                    }
                    const migrateableVersions = _.range(version + 1, TARGET_DB_VERSION + 1);
                    this.logger.info("[_processMigration] MigrateableVersions: ", migrateableVersions);
                    return Promise.serial(migrateableVersions, x => this._processVersionMigration(x));
                })
        });
    }

    private _processVersionMigration(targetVersion: number)
    {
        this.logger.info("[_processVersionMigration] target version: %s", targetVersion);

        const migrator = this._migrators[targetVersion.toString()];
        if (!migrator) {
            throw new Error(`Missing Migrator for db version ${targetVersion}`);
        }
        return Promise.resolve()
            .then(() => {
                const migratorArgs : MigratorArgs = {
                    logger: this.logger,
                    driver: this.driver,
                    executeSql: this._migratorExecuteSql.bind(this),
                    context: this._context,
                    sql: new SqlBuilder()
                }
                return migrator.handler!(migratorArgs);
            })
            .then(() => {
                return this._setDbVersion(targetVersion);
            })
    }

    private _migratorExecuteSql(sql: string, params? : any)
    {
        this.logger.info("[_migratorExecuteSql] Executing: %s, params: ", sql, params);
        return this.driver.executeSql(sql, params)
            .catch(reason => {
                this.logger.info("[_migratorExecuteSql] Failed. Reason: ", reason);
                throw reason;
            })
    }

    private _tableExists(name: string)
    {
        return this.driver.executeSql(`SHOW TABLES LIKE '${name}';`)
            .then(result => {
                return result.length > 0;
            })
    }

    private _getDbVersion()
    {
        return this._tableExists('config')
            .then(configExists => {
                if (!configExists) {
                    this.logger.warn('[_getDbVersion] Config table does not exist.');
                    return 0;
                }

                return this._context.configAccessor.getDBSchemaVersion();
            })
            ;
    }

    private _setDbVersion(version: number)
    {
        this._logger.info("[_setDbVersion] version: %s", version);

        return this._context.configAccessor.setDBSchemaVersion(version);
    }
}