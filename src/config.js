require('dotenv').config();

module.exports = {
    debug: process.env.DEBUG === 'true', // Convert string to boolean
    asyncProcessing: process.env.ASYNC_PROCESSING === 'true', // Enable/disable async processing
    viewDefinitionsFolder: process.env.VIEW_DEFINITIONS_FOLDER,
    ndjsonFilePath: process.env.NDJSON_FILE_PATH,
    duckdbFolder: process.env.DUCKDB_FOLDER,
    duckdbFileName: process.env.DUCKDB_FILE_NAME
};