import duckdb from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';
import config from './config.js';
import logger from './logger.js';
import { logFailedRecord } from './utils.js';

// Ensure DuckDB folder exists
const duckdbFolder = path.resolve(config.duckdbFolder);
if (!fs.existsSync(duckdbFolder)) {
    fs.mkdirSync(duckdbFolder, { recursive: true });
}

const dbPath = path.join(duckdbFolder, config.duckdbFileName);
let instance;
const connectionPool = []; // Pool of connections

/**
 * Initializes the DuckDB instance and connection pool.
 */
async function initialize() {
    try {
        // Check if the instance already exists (e.g., during testing)
        if (!instance) {
            instance = await duckdb.DuckDBInstance.create(dbPath);
        }

        // Check if the connection pool is already initialized
        if (connectionPool.length === 0) {
            // Use the connection pool size from the configuration
            const poolSize = config.connectionPoolSize; // Get the pool size from config
            for (let i = 0; i < poolSize; i++) {
                const connection = await instance.connect();
                connectionPool.push(connection);
            }

            logger.info(`DuckDB instance and connection pool (size: ${poolSize}) initialized successfully`);
        } else {
            logger.info('Connection pool already initialized. Skipping reinitialization.');
        }
    } catch (error) {
        logger.error('Error initializing DuckDB:', error);
        throw error;
    }
}

/**
 * Retrieves a connection from the connection pool.
 * @returns {Promise<object>} A DuckDB connection.
 */
async function getConnection() {
    if (connectionPool.length === 0) {
        throw new Error('No connections available in the pool');
    }
    return connectionPool.pop(); // Get a connection from the pool
}

/**
 * Releases a connection back to the connection pool.
 * @param {object} connection - The DuckDB connection to release.
 */
async function releaseConnection(connection) {
    connectionPool.push(connection); // Return the connection to the pool
}

/**
 * Checks if a table exists in the database.
 * @param {string} tableName - The name of the table to check.
 * @returns {Promise<boolean>} True if the table exists, false otherwise.
 */
async function tableExists(tableName) {
    const connection = await getConnection();
    try {
        const result = await connection.runAndReadAll(
            `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '${tableName}');`
        );
        return result.getRows()[0][0];
    } catch (error) {
        logger.error('Error checking if table exists:', error);
        throw error;
    } finally {
        await releaseConnection(connection);
    }
}

/**
 * Maps FHIR data types to DuckDB data types.
 * @param {string} fhirType - The FHIR data type.
 * @param {Array} tags - Additional tags for type mapping.
 * @returns {string} The corresponding DuckDB data type.
 */
function mapFhirTypeToDuckDBType(fhirType, tags = []) {
    const typeTag = tags.find(t => t.name === 'ansi/type');
    if (typeTag) return typeTag.value;

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
        base64Binary: 'BLOB',
        instant: 'TIMESTAMP',
        time: 'TIME',
        positiveInt: 'INTEGER',
        unsignedInt: 'INTEGER',
        integer64: 'BIGINT'
    };

    return typeMapping[fhirType] || 'VARCHAR';
}

/**
 * Creates a table in DuckDB if it does not already exist.
 * @param {string} tableName - The name of the table to create.
 * @param {Array} columns - The columns to include in the table.
 */
const columnTypes = {};

async function createTable(tableName, columns) {
    const connection = await getConnection();
    try {
        if (!columns || !Array.isArray(columns)) {
            throw new Error('Invalid columns: columns must be an array');
        }

        if (!await tableExists(tableName)) {
            const sequenceName = `${tableName}_id_seq`;
            await connection.run(`CREATE SEQUENCE ${sequenceName};`);

            const columnDefs = [
                `id INTEGER PRIMARY KEY DEFAULT nextval('${sequenceName}')`,
                ...columns.map(col => {
                    const dbType = mapFhirTypeToDuckDBType(col.type, col.tags);
                    return col.collection ? `${col.name} ${dbType}[]` : `${col.name} ${dbType}`;
                })
            ].join(', ');

            // Removed unique constraint on patient_id
            const query = `CREATE TABLE ${tableName} (${columnDefs});`;
            logger.debug(`Creating table with query: ${query}`);
            await connection.run(query);
            logger.info(`Table "${tableName}" created successfully.`);
        } else {
            logger.info(`Table "${tableName}" already exists. Skipping creation.`);
        }
    } catch (error) {
        logger.error('Error creating table:', error);
        throw error;
    } finally {
        await releaseConnection(connection);
    }
}

/**
 * Upserts data into a table.
 * @param {string} tableName - The name of the table.
 * @param {Array} rows - The rows to upsert.
 * @param {string} resourceKey - The resource name.
 * @returns {Promise<{inserted: number, updated: number, errors: number}>} The result of the upsert operation.
 */
async function upsertData(tableName, rows, resourceKey) {
    if (rows.length === 0) {
        logger.warn(`No rows to upsert for table ${tableName}`);
        return { inserted: 0, updated: 0, errors: 0 };
    }

    let inserted = 0;
    let updated = 0;
    let errors = 0;

    try {
        // Get table schema dynamically
        const connection = await getConnection();
        const tableSchema = await connection.runAndReadAll(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '${tableName}'
        `);
        await releaseConnection(connection);

        // Extract column names (excluding id)
        const allColumns = tableSchema.getRows()
            .map(row => row[0])
            .filter(col => col !== 'id');

        // Prepare SQL query for INSERT
        const placeholders = allColumns.map(() => '?').join(', ');
        const query = `
            INSERT INTO ${tableName} (${allColumns.join(', ')}) 
            VALUES (${placeholders});
        `;

        const { default: pLimit } = await import('p-limit');
        const limit = pLimit(config.asyncProcessing ? config.concurrencyLimit : 1);
        const batchSize = config.batchSize;

        for (let i = 0; i < rows.length; i += batchSize) {
            const batch = rows.slice(i, i + batchSize);
            logger.info(`Processing batch ${i / batchSize + 1} of ${Math.ceil(rows.length / batchSize)}`);

            await Promise.all(batch.map(row => limit(async () => {
                let connection;
                try {
                    connection = await getConnection();

                    // Validate resource key
                    if (!row[resourceKey]) {
                        logger.error(`Missing resource key in row: ${JSON.stringify(row, null, 2)}`);
                        errors++;
                        return; // Skip this row
                    }

                    // Ensure all columns are present with null defaults
                    const values = allColumns.map(col => {
                        const value = row[col];
                        return value !== undefined ? value : null;
                    });

                    // Insert the row
                    await connection.run(query, values);
                    inserted++;
                } catch (error) {
                    errors++;
                    logger.error('Error inserting row:', error.message);
                    logger.error('Failed values:', JSON.stringify({
                        patient_id: row[resourceKey],
                        values: values.map((v, i) => ({
                            column: allColumns[i],
                            value: v
                        }))
                    }, null, 2));
                    logFailedRecord(tableName, row, error);
                } finally {
                    if (connection) {
                        await releaseConnection(connection);
                    }
                }
            })));

            logger.info(`Processed batch ${i / batchSize + 1}: Upserted ${i + batch.length} of ${rows.length} rows (Inserted: ${inserted}, Updated: ${updated}, Errors: ${errors})`);
        }
    } catch (error) {
        logger.error('Error in upsertData:', error);
        throw error;
    }

    return { inserted, updated, errors };
}

/**
 * Retrieves the database handler, initializing the DuckDB instance if necessary.
 * @returns {Promise<{createTable: function, upsertData: function, tableExists: function}>} The database handler.
 */
async function getDatabaseHandler() {
    if (!instance) {
        await initialize();
    }
    return {
        createTable,
        upsertData,
        tableExists,
        getConnection, // Expose getConnection
        releaseConnection // Expose releaseConnection
    };
}

// Export individual functions
export { createTable, upsertData, tableExists };

// Export the default handler
export default getDatabaseHandler;