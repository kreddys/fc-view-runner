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
                }),
                `last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP` // Add last_updated column
            ].join(', ');

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
 * @param {string} resourceKey - The resource key (e.g., "patient_id").
 * @returns {Promise<{inserted: number, updated: number, errors: number}>} The result of the upsert operation.
 */
async function upsertData(tableName, rows, resourceKey) {
    if (rows.length === 0) {
        logger.warn(`No rows to upsert for table ${tableName}`);
        return { inserted: 0, deleted: 0, updated: 0, errors: 0 };
    }

    let inserted = 0;
    let deleted = 0;
    let updated = 0; // Track updated records
    let errors = 0;

    let connection;

    try {
        connection = await getConnection();

        // Start a transaction
        await connection.run('BEGIN TRANSACTION;');

        // Get the unique resource keys from the rows
        const resourceKeys = [...new Set(rows.map(row => row[resourceKey]))];

        // Delete existing records with the same resourceKey
        for (const key of resourceKeys) {
            // Get the count of rows before deletion
            const countBefore = await connection.runAndReadAll(
                `SELECT COUNT(*) FROM ${tableName} WHERE ${resourceKey} = ?;`,
                [key]
            );
            const countBeforeValue = Number(countBefore.getRows()[0][0]);

            // Execute the DELETE query
            const deleteQuery = `DELETE FROM ${tableName} WHERE ${resourceKey} = ?;`;
            await connection.run(deleteQuery, [key]);

            // Get the count of rows after deletion
            const countAfter = await connection.runAndReadAll(
                `SELECT COUNT(*) FROM ${tableName} WHERE ${resourceKey} = ?;`,
                [key]
            );
            const countAfterValue = Number(countAfter.getRows()[0][0]);

            // Calculate the number of rows deleted
            const rowsDeleted = countBeforeValue - countAfterValue;
            deleted += rowsDeleted;

            // If rows were deleted and new rows are being inserted, count them as updated
            if (rowsDeleted > 0 && rows.some(row => row[resourceKey] === key)) {
                updated += rowsDeleted;
            }

            logger.debug(`Deleted ${rowsDeleted} records with ${resourceKey} = ${key}`);
        }

        // Insert new records
        const tableSchema = await connection.runAndReadAll(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '${tableName}'
        `);

        // Exclude id and last_updated columns
        const allColumns = tableSchema.getRows()
            .map(row => row[0])
            .filter(col => col !== 'id' && col !== 'last_updated');

        const placeholders = allColumns.map(() => '?').join(', ');
        const insertQuery = `
            INSERT INTO ${tableName} (${allColumns.join(', ')}) 
            VALUES (${placeholders});
        `;

        for (const row of rows) {
            try {
                const values = allColumns.map(col => row[col] !== undefined ? row[col] : null);
                await connection.run(insertQuery, values);
                inserted++;
            } catch (error) {
                errors++;
                logger.error('Error inserting row:', error.message);
                logger.error('Failed row:', JSON.stringify(row, null, 2));
                logFailedRecord(tableName, row, error);
            }
        }

        // Commit the transaction
        await connection.run('COMMIT;');
        logger.info(`Processed ${rows.length} rows (Deleted: ${deleted}, Inserted: ${inserted}, Updated: ${updated}, Errors: ${errors})`);
    } catch (error) {
        // Rollback the transaction in case of errors
        if (connection) {
            await connection.run('ROLLBACK;');
        }
        errors += rows.length; // Count all rows as errors if the operation fails
        logger.error('Error in upsertData:', error);
        throw error;
    } finally {
        if (connection) {
            await releaseConnection(connection);
        }
    }

    return { inserted, deleted, updated, errors };
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