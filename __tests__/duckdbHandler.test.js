import { jest, describe, it, expect, beforeAll, beforeEach, afterAll } from '@jest/globals';
import getDatabaseHandler from '../src/duckdbHandler.js';

// Mock the logger
const logger = {
    info: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    warn: jest.fn()
};

// Replace the logger in the module with the mock
jest.mock('../src/logger.js', () => logger);

describe('DuckDBHandler', () => {
    let dbHandler;

    beforeAll(async () => {
        dbHandler = await getDatabaseHandler();
    });

    beforeEach(async () => {
        const tableName = 'test_table';
        let connection;
        try {
            // Get a connection from the pool
            connection = await dbHandler.getConnection();

            // Drop the table if it exists
            await connection.run(`DROP TABLE IF EXISTS ${tableName}`);

            // Recreate the table
            const columns = [
                { name: 'test_table_id', type: 'VARCHAR', primaryKey: true }, // Mark as primary key
                { name: 'value', type: 'VARCHAR' }
            ];
            await dbHandler.createTable(tableName, columns);
        } catch (error) {
            logger.error('Error resetting table:', error); // Use the mocked logger
            throw error;
        } finally {
            // Release the connection back to the pool
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    it('should create a table with a primary key', async () => {
        const tableName = 'test_table';
        const columns = [
            { name: 'test_table_id', type: 'VARCHAR', primaryKey: true }, // Mark as primary key
            { name: 'value', type: 'VARCHAR' }
        ];

        await dbHandler.createTable(tableName, columns);
        const exists = await dbHandler.tableExists(tableName);
        expect(exists).toBe(true);
    });

    it('should upsert data into a table', async () => {
        const tableName = 'test_table';
        const rows = [
            { test_table_id: '1', value: 'test1' }, // Use the correct primary key column name
            { test_table_id: '2', value: 'test2' }
        ];

        // Ensure the table is empty before upserting
        let connection;
        try {
            connection = await dbHandler.getConnection();
            await connection.run(`DELETE FROM ${tableName}`);
        } catch (error) {
            logger.error('Error clearing table:', error);
            throw error;
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }

        const result = await dbHandler.upsertData(tableName, rows, 'test_table_id');
        expect(result.inserted).toBe(2); // Expect 2 rows to be inserted
        expect(result.errors).toBe(0);
    });

    it('should handle duplicate primary keys during upsert', async () => {
        const tableName = 'test_table';

        // Insert initial data
        const initialRows = [
            { test_table_id: '1', value: 'test1' } // Use the correct primary key column name
        ];
        await dbHandler.upsertData(tableName, initialRows, 'test_table_id');

        // Update the existing row
        const updatedRows = [
            { test_table_id: '1', value: 'updated_value' } // Use the correct primary key column name
        ];

        const result = await dbHandler.upsertData(tableName, updatedRows, 'test_table_id');
        expect(result.updated).toBe(1); // Expect 1 row to be updated
    });
});