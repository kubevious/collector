import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger';

import { Context } from '../../context';

import moment from 'moment';

const CronJob = require('cron').CronJob

import { Database } from '../../db';
import { ProcessingTrackerScoper } from '@kubevious/helper-backend';
import { PartitionUtils } from '@kubevious/data-models';

export class HistoryCleanupProcessor
{
    private _logger : ILogger;
    private _context : Context

    private _database : Database;
    private _days : number = 15;

    private _startupDate? : moment.Moment;
    private _lastCleanupDate? : moment.Moment;
    
    private _readyToProcess: boolean = false;
    private _isProcessing : boolean = false;

    private _tableNames : string[];

    constructor(context: Context)
    {
        this._context = context;
        this._logger = context.logger.sublogger('HistoryCleanupProcessor');
        this._database = context.database;

        this._tableNames = [
            this._database.snapshots.SnapshotConfigs,
            this._database.snapshots.Snapshots,
            this._database.snapshots.SnapItems,
            this._database.snapshots.DiffItems,
            this._database.snapshots.DeltaItems,
            this._database.snapshots.Timeline,
        ].map(x => x.tableName);
    }

    get logger() {
        return this._logger;
    }

    get driver() {
        return this._database.driver;
    }

    init()
    {
        this._startupDate = moment();

        this._database.onConnect(() => {
            this._setupCronJob();
        });
    }

    private _setupCronJob()
    {
        const schedule = '* 0/15 0-2 * * *';
        // const schedule = '*/1 * * * *';
        const cleanupJob = new CronJob(schedule, () => {
            this._processSchedule();
        })
        cleanupJob.start();
    }

    private _processSchedule()
    {
        const now = moment();
        this.logger.info('[_processSchedule] now: %s', now);

        if (now.diff(this._startupDate, 'minutes') < 15) {
            this.logger.info('[_processSchedule] skipped, waiting 15 minutes post startup');
            return;
        }
        if (this._lastCleanupDate)
        {
            if (now.diff(this._lastCleanupDate, 'hours') < 20) {
                this.logger.info('[_processSchedule] skipped, processed within last 20 hours');
                return;
            }
        }

        this.logger.info('[_processSchedule] will execute');
        this._initiateCleanup();
    }

    private _initiateCleanup()
    {
        this._logger.warn('[_initiateCleanup] this._readyToProcess: %s', this._readyToProcess);

        if (this._readyToProcess) {
            this._logger.warn('[_initiateCleanup] Skipped. Is Triggered.');
            return;
        }
        if (this._isProcessing) {
            this._logger.warn('[_initiateCleanup] Skipped. Is Processing.');
            return;
        }

        this._readyToProcess = true;
        this._context.facadeRegistry.jobDampener.pause();

        if (!this._context.facadeRegistry.jobDampener.isBusy) {
            this.tryProcess();
        } else {
            this._logger.warn('[_initiateCleanup] JobDampener is busy now.');
        }
    }

    tryProcess()
    {
        if (!this._readyToProcess) {
            this._logger.info("[tryProcess] Begin. Not ready to process");
            return;
        }
        if (this._isProcessing) {
            this._logger.info("[tryProcess] Begin. Is already processing");
            return;
        }
        this._isProcessing = true;
        this._readyToProcess = false;

        this._logger.info("[tryProcess] Begin");

        Promise.resolve(null)
            .then(() => this._processCleanupNow())
            .catch(reason => {
                this._logger.error("[tryProcess] REASON: ", reason);
            })
            .finally(() => {
                this._isProcessing = false;
                this._context.facadeRegistry.jobDampener.resume();
            })
    }

    private _processCleanupNow()
    {
        this._logger.info('[_processCleanupNow] Begin');

        this._lastCleanupDate = moment();

        const cutoffDate = moment().subtract(this._days, 'days');
        this._logger.info('[_processCleanupNow] Cutoff Date: %s', cutoffDate);

        const cutoffPartition = PartitionUtils.getPartitionIdFromDate(cutoffDate.toDate());
        this._logger.info('[_processCleanupNow] CutoffPartition: %s', cutoffPartition);

        return this._executeCleanup(cutoffPartition)
            .then(() => {
                this._logger.info('[_processCleanupNow] End');
            })
            .catch(reason => {
                this._logger.error('[_processCleanupNow] FAILED: ', reason);
            });
    }

    private _executeCleanup(cutoffPartition: number)
    {
        return this._context.tracker.scope("HistoryCleanupProcessor", (innerTracker) => {
            
                return Promise.resolve()
                    .then(() => this._outputDBUsage('pre-cleanup', innerTracker))
                    .then(() => this._cleanupHistoryTables(innerTracker, cutoffPartition))
                    .then(() => this._outputDBUsage('post-cleanup', innerTracker))
            })
    }

    private _cleanupHistoryTables(tracker: ProcessingTrackerScoper, cutoffPartition: number)
    {
        this._logger.info('[_cleanupHistoryTables] Running...');

        return tracker.scope("_cleanupHistoryTables", () => {
            return Promise.serial(this._tableNames, x => this._cleanupHistoryTable(x, cutoffPartition));
        });
    }

    private _cleanupHistoryTable(tableName: string, cutoffPartition: number)
    {
        this._logger.info('[_cleanupHistoryTable] Table: %s', tableName);
        return this.driver.partitionManager.queryPartitions(tableName)
            .then(partitions => {
                // this._logger.info('[_cleanupHistoryTable] Table: %s, Current Partitions: ', tableName, partitions);
                
                const partitionIds = partitions.map(x => x.value - 1);

                const partitionsToDelete = partitionIds.filter(x => (x <= cutoffPartition));
                this._logger.info('[_cleanupHistoryTable] table: %s, partitionsToDelete:', tableName, partitionsToDelete);

                return Promise.serial(partitionsToDelete, x => this._deletePartition(tableName, x));
            });
    }

    private _deletePartition(tableName: string, partitionId: number)
    {
        const partitionName = PartitionUtils.partitionName(partitionId);

        this._logger.info('[_deletePartition] Table: %s, Partition: %s', tableName, partitionName);
        return this.driver.partitionManager.dropPartition(tableName, partitionName);
    }

    private _outputDBUsage(stage: string, tracker: ProcessingTrackerScoper)
    {
        return tracker.scope("_outputDBUsage", () => {
            return this._outputDbSize(stage)
        });
    }

    private _outputDbSize(stage: string)
    {
        const sql = `SELECT \`TABLE_NAME\`, \`TABLE_ROWS\`, ((data_length + index_length) / 1024 / 1024 ) AS size FROM information_schema.TABLES WHERE table_schema = "${process.env.MYSQL_DB}"`
        return this.driver.executeSql(sql)
            .then(result => {
                result = _.orderBy(result, ['size'], ['desc']);
                for(const x of result)
                {
                    this._logger.info('[_outputDbSize] %s, Table: %s, Rows: %s, Size: %s MB', stage, x.TABLE_NAME, x.TABLE_ROWS, x.size);
                }
            });
    }

}