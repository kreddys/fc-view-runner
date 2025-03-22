import dotenv from 'dotenv';
dotenv.config();

export default {
    debug: process.env.DEBUG === 'true', // Convert string to boolean
    asyncProcessing: process.env.ASYNC_PROCESSING === 'true', // Enable/disable async processing
    viewDefinitionsFolder: process.env.VIEW_DEFINITIONS_FOLDER,
    ndjsonFilePath: process.env.NDJSON_FILE_PATH,
    duckdbFolder: process.env.DUCKDB_FOLDER,
    duckdbFileName: process.env.DUCKDB_FILE_NAME,
    logsFolder: process.env.LOGS_FOLDER || './logs', // Folder for log files
    connectionPoolSize: parseInt(process.env.CONNECTION_POOL_SIZE || '10', 10), // Number of connections in the pool
    concurrencyLimit: parseInt(process.env.CONCURRENCY_LIMIT || '10', 10), // Concurrency limit for async processing
    batchSize: parseInt(process.env.BATCH_SIZE || '1000', 10), // Batch size for processing
    logLevel: process.env.LOG_LEVEL || 'info', // Logging level (e.g., debug, info, warn, error)
    bulkExportFolder: process.env.BULK_EXPORT_FOLDER,
};