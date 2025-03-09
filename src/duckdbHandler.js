const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs');
const config = require('./config');

const duckdbFolder = path.join(__dirname, '../data/duckdb');
if (!fs.existsSync(duckdbFolder)) {
    fs.mkdirSync(duckdbFolder, { recursive: true });
}

const dbPath = path.join(duckdbFolder, 'fhir_data.db');
let instance;
let connection;

function logDebug(message) {
    if (config.debug) {
        console.log(message);
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
    // Check for explicit type override in tags
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

async function createTable(tableName, viewDef) {
    try {
        const { columns, nestedSelects } = viewDef;

        // Create main table
        if (!await tableExists(tableName)) {
            const columnDefs = columns.map(col => {
                const dbType = mapFhirTypeToDuckDBType(col.type, col.tags);
                if (col.collection) {
                    return `${col.name} ${dbType}[]`; // Array type for collection columns
                }
                return `${col.name} ${dbType}`;
            }).join(', ');

            const query = `CREATE TABLE ${tableName} (${columnDefs});`;
            logDebug(`Creating table with query: ${query}`);
            await connection.run(query);
        }

        // Create tables for nested selects
        for (const nested of nestedSelects) {
            const nestedTableName = `${tableName}_${nested.path.replace(/\./g, '_')}`;
            await createNestedTable(nestedTableName, nested, tableName);
        }
    } catch (error) {
        console.error('Error creating table:', error);
        throw error;
    }
}

async function createNestedTable(tableName, nestedSelect, parentTable) {
    if (!await tableExists(tableName)) {
        const columnDefs = nestedSelect.columns.map(col => {
            const dbType = mapFhirTypeToDuckDBType(col.type, col.tags);
            return `${col.name} ${col.collection ? `${dbType}[]` : dbType}`;
        });

        // Add foreign key reference to parent table
        columnDefs.push(`${parentTable}_id VARCHAR REFERENCES ${parentTable}(id)`);

        const query = `CREATE TABLE ${tableName} (
            id VARCHAR PRIMARY KEY,
            ${columnDefs.join(', ')}
        );`;

        await connection.run(query);
    }
}

async function upsertData(tableName, rows, primaryKey) {
    if (rows.length === 0) return;

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
                await connection.run(query, values);
                logDebug(`Upserted row with ${primaryKey}: ${row[primaryKey]}`);
            } catch (error) {
                console.error('Error upserting row:', error);
                console.error('Row data:', JSON.stringify(row, null, 2));
                throw error;
            }
        }
    } catch (error) {
        throw error;
    }
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