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
        const tablesToReset = ['test_table', 'patient_identifier']; // Add all tables to reset here
        let connection;
        try {
            // Get a connection from the pool
            connection = await dbHandler.getConnection();

            // Drop and recreate each table
            for (const tableName of tablesToReset) {
                // Drop the table if it exists
                await connection.run(`DROP TABLE IF EXISTS ${tableName}`);

                // Drop the sequence if it exists
                const sequenceName = `${tableName}_id_seq`;
                await connection.run(`DROP SEQUENCE IF EXISTS ${sequenceName}`);

                // Recreate the table
                const columns = tableName === 'patient_identifier'
                    ? [
                        { name: 'patient_id', type: 'VARCHAR' }, // Resource key
                        { name: 'identifier_type', type: 'VARCHAR' },
                        { name: 'identifier_value', type: 'VARCHAR' }
                    ]
                    : [
                        { name: 'test_table_id', type: 'VARCHAR' }, // No primary key here
                        { name: 'value', type: 'VARCHAR' }
                    ];

                await dbHandler.createTable(tableName, columns);
            }
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

    afterAll(async () => {
        const tablesToDelete = ['test_table', 'patient_identifier']; // Add all tables to delete here
        let connection;
        try {
            // Get a connection from the pool
            connection = await dbHandler.getConnection();

            // Drop each table
            for (const tableName of tablesToDelete) {
                await connection.run(`DROP TABLE IF EXISTS ${tableName}`);
                const sequenceName = `${tableName}_id_seq`;
                await connection.run(`DROP SEQUENCE IF EXISTS ${sequenceName}`);
            }
        } catch (error) {
            logger.error('Error cleaning up tables:', error); // Use the mocked logger
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
            { name: 'test_table_id', type: 'VARCHAR' }, // No primary key here
            { name: 'value', type: 'VARCHAR' }
        ];

        await dbHandler.createTable(tableName, columns);
        const exists = await dbHandler.tableExists(tableName);
        expect(exists).toBe(true);
    });

    it('should insert data into a table', async () => {
        const tableName = 'test_table';
        const rows = [
            { test_table_id: '1', value: 'test1' }, // Use the correct column name
            { test_table_id: '2', value: 'test2' }
        ];

        // Ensure the table is empty before inserting
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

    it('should insert multiple records with the same resourceName_id', async () => {
        const tableName = 'patient_identifier';
        const rows = [
            { patient_id: '123', identifier_type: 'SSN', identifier_value: '123-45-6789' },
            { patient_id: '123', identifier_type: 'MRN', identifier_value: '987654' }
        ];

        const result = await dbHandler.upsertData(tableName, rows, 'patient_id');
        expect(result.inserted).toBe(2); // Expect 2 rows to be inserted
        expect(result.errors).toBe(0);
    });

    it('should handle missing resourceName_id column', async () => {
        const tableName = 'test_table';
        const rows = [
            { value: 'test1' }, // Missing primary key
            { value: 'test2' }
        ];

        const result = await dbHandler.upsertData(tableName, rows, 'test_table_id');
        expect(result.errors).toBe(2); // Expect 2 errors due to missing primary key
    });

    it('should process large batches of records', async () => {
        const tableName = 'test_table';
        const rows = Array.from({ length: 2000 }, (_, i) => ({
            test_table_id: `${i + 1}`,
            value: `value${i + 1}`
        }));

        const result = await dbHandler.upsertData(tableName, rows, 'test_table_id');
        expect(result.inserted).toBe(2000); // Expect 2000 rows to be inserted
        expect(result.errors).toBe(0);
    });
});