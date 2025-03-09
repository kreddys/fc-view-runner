const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs');
const config = require('./config');

// Ensure the data/duckdb folder exists
const duckdbFolder = path.join(__dirname, '../data/duckdb');
if (!fs.existsSync(duckdbFolder)) {
    fs.mkdirSync(duckdbFolder, { recursive: true });
}

// Initialize DuckDB with a file path
const dbPath = path.join(duckdbFolder, 'fhir_data.db');
let instance;
let connection;

async function initialize() {
    try {
        instance = await duckdb.DuckDBInstance.create(dbPath);
        connection = await instance.connect();
        logDebug('DuckDB instance and connection initialized successfully');
    } catch (error) {
        console.error('Error initializing DuckDB:', error);
        throw error;
    }
}

function logDebug(message) {
    if (config.debug) {
        console.log(message);
    }
}

/**
 * Check if a table exists in DuckDB.
 * @param {string} tableName - Name of the table.
 * @returns {Promise<boolean>} - True if the table exists, false otherwise.
 */
async function tableExists(tableName) {
    try {
        const result = await connection.runAndReadAll(
            `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '${tableName}');`
        );
        return result.getRows()[0][0];
    } catch (error) {
        console.error('Error checking if table exists:', error);
        throw error;
    }
}

/**
 * Map FHIR types to DuckDB types.
 * @param {string} fhirType - The FHIR type.
 * @returns {string} - The corresponding DuckDB type.
 */
function mapFhirTypeToDuckDBType(fhirType) {
    const typeMapping = {
        boolean: 'BOOLEAN',
        integer: 'INTEGER',
        decimal: 'DOUBLE',
        date: 'DATE',
        dateTime: 'TIMESTAMP',
        string: 'VARCHAR',
        uri: 'VARCHAR',
        code: 'VARCHAR',
        markdown: 'VARCHAR',
        id: 'VARCHAR',
        url: 'VARCHAR',
        uuid: 'VARCHAR',
    };

    return typeMapping[fhirType] || 'VARCHAR';
}

/**
 * Create a table in DuckDB based on the column definitions.
 * @param {string} tableName - Name of the table.
 * @param {Array} columns - Array of objects containing path, name, and type.
 */
async function createTable(tableName, columns) {
    try {
        const tableExistsResult = await tableExists(tableName);
        if (tableExistsResult) {
            logDebug(`Table "${tableName}" already exists. Skipping creation.`);
            return;
        }

        const idColumnName = `${tableName}_id`;
        const columnDefinitions = columns
            .map((col) => {
                if (col.name === idColumnName) {
                    return `${col.name} ${mapFhirTypeToDuckDBType(col.type)} PRIMARY KEY`;
                }
                return `${col.name} ${mapFhirTypeToDuckDBType(col.type)}`;
            })
            .join(', ');

        const query = `CREATE TABLE ${tableName} (${columnDefinitions});`;
        logDebug(`Creating table with query: ${query}`);

        await connection.run(query);
        logDebug('Table created successfully.');
    } catch (error) {
        console.error('Error creating table:', error);
        throw error;
    }
}

/**
 * Insert or update rows in the DuckDB table.
 * @param {string} tableName - Name of the table.
 * @param {Array} rows - Array of objects representing rows of data.
 * @param {string} primaryKey - The primary key column (e.g., "observation_id" or "patient_id").
 */
async function upsertData(tableName, rows, primaryKey) {
    if (rows.length === 0) return;

    try {
        const columns = Object.keys(rows[0]);
        const placeholders = columns.map(() => '?').join(', ');
        const updateClause = columns.map((col) => `${col} = EXCLUDED.${col}`).join(', ');

        const query = `
            INSERT INTO ${tableName} (${columns.join(', ')}) 
            VALUES (${placeholders})
            ON CONFLICT (${primaryKey}) 
            DO UPDATE SET ${updateClause};
        `;

        logDebug(`Upserting data into table: ${tableName}`);

        for (const row of rows) {
            const values = columns.map(col => row[col]);
            try {
                await connection.run(query, values);
                logDebug(`Successfully upserted row: ${JSON.stringify(values, null, 2)}`);
            } catch (error) {
                console.error('Error upserting row:', error);
                console.error('Table:', tableName);
                console.error('Row data:', JSON.stringify(row, null, 2));
                console.error('Query:', query);
                console.error('Values:', JSON.stringify(values, null, 2));
                throw error;
            }
        }

        logDebug('All data upserted successfully.');
    } catch (error) {
        throw error;
    }
}

// Initialize the database connection when the module is loaded
initialize().catch(console.error);

// Export an async function to ensure the database is initialized
async function getDatabaseHandler() {
    if (!connection) {
        await initialize();
    }
    return {
        createTable,
        upsertData
    };
}

module.exports = getDatabaseHandler;