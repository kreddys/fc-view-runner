const duckdb = require('duckdb');
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
const db = new duckdb.Database(dbPath);

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
function tableExists(tableName) {
    return new Promise((resolve, reject) => {
        const connection = db.connect();
        const query = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '${tableName}');`;

        connection.all(query, (err, rows) => {
            if (err) {
                console.error('Error checking if table exists:', err);
                reject(err);
            } else {
                resolve(rows[0].exists);
            }
        });
    });
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

    return typeMapping[fhirType] || 'VARCHAR'; // Default to VARCHAR if type is unknown
}

/**
 * Create a table in DuckDB based on the column definitions.
 * @param {string} tableName - Name of the table.
 * @param {Array} columns - Array of objects containing path, name, and type.
 */
async function createTable(tableName, columns) {
    const tableExistsResult = await tableExists(tableName);
    if (tableExistsResult) {
        logDebug(`Table "${tableName}" already exists. Skipping creation.`);
        return;
    }

    const columnDefinitions = columns
        .map((col) => `${col.name} ${mapFhirTypeToDuckDBType(col.type)}`)
        .join(', ');

    const query = `CREATE TABLE ${tableName} (${columnDefinitions});`;

    logDebug(`Creating table with query: ${query}`);

    return new Promise((resolve, reject) => {
        db.run(query, (err) => {
            if (err) {
                console.error('Error creating table:', err);
                reject(err);
            } else {
                logDebug('Table created successfully.');
                resolve();
            }
        });
    });
}

/**
 * Insert or update rows in the DuckDB table.
 * @param {string} tableName - Name of the table.
 * @param {Array} rows - Array of objects representing rows of data.
 * @param {string} primaryKey - The primary key column (e.g., "observation_id" or "patient_id").
 */
function upsertData(tableName, rows, primaryKey) {
    return new Promise((resolve, reject) => {
        const connection = db.connect();

        const columns = Object.keys(rows[0] || []);
        const placeholders = columns.map(() => '?').join(', ');
        const updateClause = columns.map((col) => `${col} = EXCLUDED.${col}`).join(', ');

        const query = `
      INSERT INTO ${tableName} (${columns.join(', ')}) 
      VALUES (${placeholders})
      ON CONFLICT (${primaryKey}) 
      DO UPDATE SET ${updateClause};
    `;

        logDebug(`Upserting data into table: ${tableName}`);
        logDebug(`Upsert query: ${query}`);

        let hasError = false;

        rows.forEach((row) => {
            const values = columns.map((col) => row[col]);
            logDebug(`Upserting row: ${JSON.stringify(values, null, 2)}`);

            connection.run(query, values, (err) => {
                if (err) {
                    hasError = true;
                    console.error('Error upserting row:', err);
                    console.error('Table:', tableName);
                    console.error('Row data:', JSON.stringify(row, null, 2));
                    console.error('Query:', query);
                    console.error('Values:', JSON.stringify(values, null, 2));
                    reject(err);
                }
            });
        });

        // if (!hasError) {
        //     logDebug('Data upserted successfully.');
        //     resolve();
        // }
    });
}

module.exports = {
    createTable,
    upsertData,
};