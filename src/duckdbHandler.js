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
let connection;

function logDebug(message) {
    if (config.debug) {
        console.log(`[DEBUG] ${message}`);
    }
}

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

        for (const row of rows) {
            const values = columns.map(col => {
                const value = row[col];
                return Array.isArray(value) ? JSON.stringify(value) : value;
            });

            try {
                // Check if the primary key already exists in the table
                const existsQuery = `SELECT 1 FROM ${tableName} WHERE ${primaryKey} = ?`;
                const existsResult = await connection.runAndReadAll(existsQuery, [row[primaryKey]]);

                await connection.run(query, values);

                if (existsResult.getRows().length > 0) {
                    updated++;
                } else {
                    inserted++;
                }

                logDebug(`Upserted row with ${primaryKey}: ${row[primaryKey]}`);
            } catch (error) {
                errors++;
                console.error('Error upserting row:', error);
                console.error('Row data:', JSON.stringify(row, null, 2));
            }
        }
    } catch (error) {
        throw error;
    }

    return { inserted, updated, errors };
}

async function getDatabaseHandler() {
    if (!connection) {
        await initialize();
    }
    return {
        createTable,
        upsertData,
        tableExists
    };
}

module.exports = getDatabaseHandler;