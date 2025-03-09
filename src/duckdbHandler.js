const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs');
const config = require('./config');
const logger = require('./logger');
const { logFailedRecord } = require('./utils');

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
        instance = await duckdb.DuckDBInstance.create(dbPath);

        // Create a pool of connections
        for (let i = 0; i < config.connectionPoolSize; i++) {
            const connection = await instance.connect();
            connectionPool.push(connection);
        }

        logger.info('DuckDB instance and connection pool initialized successfully');
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
async function createTable(tableName, columns) {
    const connection = await getConnection();
    try {
        if (!columns || !Array.isArray(columns)) {
            throw new Error('Invalid columns: columns must be an array');
        }

        if (!await tableExists(tableName)) {
            const columnDefs = columns.map(col => {
                const dbType = mapFhirTypeToDuckDBType(col.type, col.tags);
                return col.collection ? `${col.name} ${dbType}[]` : `${col.name} ${dbType}`;
            }).join(', ');

            const primaryKey = `${tableName}_id`;
            const query = `CREATE TABLE ${tableName} (${columnDefs}, PRIMARY KEY (${primaryKey}));`;
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
 * @param {string} primaryKey - The primary key column.
 * @returns {Promise<{inserted: number, updated: number, errors: number}>} The result of the upsert operation.
 */
async function upsertData(tableName, rows, primaryKey) {
    if (rows.length === 0) {
        logger.warn(`No rows to upsert for table ${tableName}`);
        return { inserted: 0, updated: 0, errors: 0 };
    }

    let inserted = 0;
    let updated = 0;
    let errors = 0;

    try {
        const columns = Object.keys(rows[0]);
        const placeholders = columns.map(() => '?').join(', ');
        const updateClause = columns
            .filter(col => col !== primaryKey)
            .map(col => `${col} = EXCLUDED.${col}`)
            .join(', ');

        const query = `
            INSERT INTO ${tableName} (${columns.join(', ')}) 
            VALUES (${placeholders})
            ON CONFLICT (${primaryKey}) 
            DO UPDATE SET ${updateClause};
        `;

        const { default: pLimit } = await import('p-limit');
        const limit = pLimit(config.asyncProcessing ? config.concurrencyLimit : 1); // Control concurrency
        const batchSize = config.batchSize; // Process rows in batches

        for (let i = 0; i < rows.length; i += batchSize) {
            const batch = rows.slice(i, i + batchSize);
            logger.info(`Processing batch ${i / batchSize + 1} of ${Math.ceil(rows.length / batchSize)}`);

            await Promise.all(batch.map(row => limit(async () => {
                const connection = await getConnection(); // Get a connection from the pool
                const values = columns.map(col => {
                    const value = row[col];
                    return Array.isArray(value) ? JSON.stringify(value) : value;
                });

                try {
                    // Validate primary key
                    if (!row[primaryKey]) {
                        logger.error(`Missing primary key in row: ${JSON.stringify(row, null, 2)}`);
                        errors++;
                        return;
                    }

                    // Execute the upsert query
                    await connection.run(query, values);

                    // Check if the primary key already exists in the table
                    const existsQuery = `SELECT 1 FROM ${tableName} WHERE ${primaryKey} = ?`;
                    const existsResult = await connection.runAndReadAll(existsQuery, [row[primaryKey]]);

                    if (existsResult.getRows().length > 0) {
                        updated++;
                        logger.debug(`Updated row with ${primaryKey}: ${row[primaryKey]}`);
                    } else {
                        inserted++;
                        logger.debug(`Inserted row with ${primaryKey}: ${row[primaryKey]}`);
                    }
                } catch (error) {
                    errors++;
                    logger.error('Error upserting row:', error.message);
                    logger.error('Row data:', JSON.stringify(row, null, 2));

                    // Log the failed record to the log file
                    logFailedRecord(tableName, row, error);
                } finally {
                    await releaseConnection(connection); // Release the connection back to the pool
                }
            })));

            // Log batch status
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
        tableExists
    };
}

module.exports = getDatabaseHandler;