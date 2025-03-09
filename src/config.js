require('dotenv').config(); // Load environment variables from .env file

module.exports = {
    debug: process.env.DEBUG === 'true', // Convert string to boolean
    viewDefinitionsFolder: process.env.VIEW_DEFINITIONS_FOLDER,
    ndjsonFilePath: process.env.NDJSON_FILE_PATH,
    duckdbFolder: process.env.DUCKDB_FOLDER,
    duckdbFileName: process.env.DUCKDB_FILE_NAME
};