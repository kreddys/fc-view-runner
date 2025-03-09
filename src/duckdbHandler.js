const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs');
const config = require('./config');

const duckdbFolder = path.resolve(config.duckdbFolder);
if (!fs.existsSync(duckdbFolder)) {
    fs.mkdirSync(duckdbFolder, { recursive: true });
}

const dbPath = path.join(duckdbFolder, config.duckdbFileName);
let instance;
const connectionPool = []; // Pool of connections

function logDebug(message) {
    if (config.debug) {
        console.log(`[DEBUG] ${new Date().toISOString()} - ${message}`);
    }
}

async function initialize() {
    try {
        instance = await duckdb.DuckDBInstance.create(dbPath);

        // Create a pool of connections
        for (let i = 0; i < config.connectionPoolSize; i++) {
            const connection = await instance.connect();
            connectionPool.push(connection);
        }

        logDebug('DuckDB instance and connection pool initialized successfully');
    } catch (error) {
        console.error('Error initializing DuckDB:', error);
        throw error;
    }
}

async function getConnection() {
    if (connectionPool.length === 0) {
        throw new Error('No connections available in the pool');
    }
    return connectionPool.pop(); // Get a connection from the pool
}

async function releaseConnection(connection) {
    connectionPool.push(connection); // Return the connection to the pool
}

async function tableExists(tableName) {
    const connection = await getConnection();
    try {
        const result = await connection.runAndReadAll(
            `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '${tableName}');`
        );
        return result.getRows()[0][0];
    } catch (error) {
        console.error('Error checking if table exists:', error);
        throw error;
    } finally {
        await releaseConnection(connection);
    }
}

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
            logDebug(`Creating table with query: ${query}`);
            await connection.run(query);
            console.log(`Table "${tableName}" created successfully.`);
        } else {
            console.log(`Table "${tableName}" already exists. Skipping creation.`);
        }
    } catch (error) {
        console.error('Error creating table:', error);
        throw error;
    } finally {
        await releaseConnection(connection);
    }
}

async function upsertData(tableName, rows, primaryKey) {
    if (rows.length === 0) {
        console.warn(`No rows to upsert for table ${tableName}`);
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
            console.log(`Processing batch ${i / batchSize + 1} of ${Math.ceil(rows.length / batchSize)}`);

            await Promise.all(batch.map(row => limit(async () => {
                const connection = await getConnection(); // Get a connection from the pool
                const values = columns.map(col => {
                    const value = row[col];
                    return Array.isArray(value) ? JSON.stringify(value) : value;
                });

                try {
                    // Validate primary key
                    if (!row[primaryKey]) {
                        console.error(`Missing primary key in row: ${JSON.stringify(row, null, 2)}`);
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
                        logDebug(`Updated row with ${primaryKey}: ${row[primaryKey]}`);
                    } else {
                        inserted++;
                        logDebug(`Inserted row with ${primaryKey}: ${row[primaryKey]}`);
                    }
                } catch (error) {
                    errors++;
                    console.error('Error upserting row:', error.message);
                    console.error('Error stack:', error.stack);
                    console.error('Row data:', JSON.stringify(row, null, 2));
                } finally {
                    await releaseConnection(connection); // Release the connection back to the pool
                }
            })));

            // Log batch status
            console.log(`Processed batch ${i / batchSize + 1}: Upserted ${i + batch.length} of ${rows.length} rows (Inserted: ${inserted}, Updated: ${updated}, Errors: ${errors})`);
        }
    } catch (error) {
        console.error('Error in upsertData:', error);
        throw error;
    }

    return { inserted, updated, errors };
}

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