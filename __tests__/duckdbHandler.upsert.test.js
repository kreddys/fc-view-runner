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

describe('DuckDBHandler - Upsert Logic', () => {
    let dbHandler;

    beforeAll(async () => {
        dbHandler = await getDatabaseHandler();
    });

    beforeEach(async () => {
        // Reset the test table before each test
        const tableName = 'test_table';
        let connection;
        try {
            connection = await dbHandler.getConnection();
            await connection.run(`DROP TABLE IF EXISTS ${tableName}`);
            await connection.run(`DROP SEQUENCE IF EXISTS ${tableName}_id_seq`);

            // Recreate the table
            const columns = [
                { name: 'test_table_id', type: 'VARCHAR' }, // Resource key
                { name: 'value', type: 'VARCHAR' }
            ];
            await dbHandler.createTable(tableName, columns);
        } catch (error) {
            logger.error('Error resetting table:', error);
            throw error;
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    afterAll(async () => {
        // Clean up the test table after all tests
        const tableName = 'test_table';
        let connection;
        try {
            connection = await dbHandler.getConnection();
            await connection.run(`DROP TABLE IF EXISTS ${tableName}`);
            await connection.run(`DROP SEQUENCE IF EXISTS ${tableName}_id_seq`);
        } catch (error) {
            logger.error('Error cleaning up table:', error);
            throw error;
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    it('should delete existing records with the same resourceKey and insert new records', async () => {
        const tableName = 'test_table';
        const resourceKey = 'test_table_id';

        // Insert initial records
        const initialRows = [
            { test_table_id: '1', value: 'initial1' },
            { test_table_id: '2', value: 'initial2' }
        ];
        const initialResult = await dbHandler.upsertData(tableName, initialRows, resourceKey);
        expect(initialResult.inserted).toBe(2); // 2 records inserted
        expect(initialResult.deleted).toBe(0); // No records deleted

        // Verify the initial records are in the table
        let connection;
        try {
            connection = await dbHandler.getConnection();
            const initialRecords = await connection.runAndReadAll(`SELECT * FROM ${tableName}`);
            const transformedInitialRows = initialRecords.getRows().map(row => ({
                test_table_id: row[1],
                value: row[2]
            }));
            expect(transformedInitialRows).toHaveLength(2);
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }

        // Upsert new records with the same resourceKey
        const newRows = [
            { test_table_id: '1', value: 'updated1' },
            { test_table_id: '3', value: 'new3' }
        ];
        const newResult = await dbHandler.upsertData(tableName, newRows, resourceKey);
        expect(newResult.deleted).toBe(1); // 1 record deleted (test_table_id = '1')
        expect(newResult.inserted).toBe(2); // 2 records inserted

        // Verify the final state of the table
        try {
            connection = await dbHandler.getConnection();
            const finalRecords = await connection.runAndReadAll(`SELECT * FROM ${tableName}`);
            const transformedFinalRows = finalRecords.getRows().map(row => ({
                test_table_id: row[1],
                value: row[2]
            }));

            expect(transformedFinalRows).toHaveLength(3); // 3 records in total
            expect(transformedFinalRows).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({ test_table_id: '1', value: 'updated1' }), // Updated record
                    expect.objectContaining({ test_table_id: '2', value: 'initial2' }), // Unaffected record
                    expect.objectContaining({ test_table_id: '3', value: 'new3' }) // New record
                ])
            );
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    it('should handle upsert with no existing records', async () => {
        const tableName = 'test_table';
        const resourceKey = 'test_table_id';

        // Upsert new records
        const newRows = [
            { test_table_id: '1', value: 'new1' },
            { test_table_id: '2', value: 'new2' }
        ];
        const result = await dbHandler.upsertData(tableName, newRows, resourceKey);
        expect(result.deleted).toBe(0); // No records deleted
        expect(result.inserted).toBe(2); // 2 records inserted

        // Verify the final state of the table
        let connection;
        try {
            connection = await dbHandler.getConnection();
            const finalRecords = await connection.runAndReadAll(`SELECT * FROM ${tableName}`);
            const transformedFinalRows = finalRecords.getRows().map(row => ({
                test_table_id: row[1],
                value: row[2]
            }));

            expect(transformedFinalRows).toHaveLength(2); // 2 records in total
            expect(transformedFinalRows).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({ test_table_id: '1', value: 'new1' }),
                    expect.objectContaining({ test_table_id: '2', value: 'new2' })
                ])
            );
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    it('should handle upsert with no new records', async () => {
        const tableName = 'test_table';
        const resourceKey = 'test_table_id';

        // Insert initial records
        const initialRows = [
            { test_table_id: '1', value: 'initial1' },
            { test_table_id: '2', value: 'initial2' }
        ];
        await dbHandler.upsertData(tableName, initialRows, resourceKey);

        // Upsert with no new records
        const newRows = [];
        const result = await dbHandler.upsertData(tableName, newRows, resourceKey);
        expect(result.deleted).toBe(0); // No records deleted
        expect(result.inserted).toBe(0); // No records inserted

        // Verify the final state of the table
        let connection;
        try {
            connection = await dbHandler.getConnection();
            const finalRecords = await connection.runAndReadAll(`SELECT * FROM ${tableName}`);
            const transformedFinalRows = finalRecords.getRows().map(row => ({
                test_table_id: row[1],
                value: row[2]
            }));

            expect(transformedFinalRows).toHaveLength(2); // 2 records in total
            expect(transformedFinalRows).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({ test_table_id: '1', value: 'initial1' }),
                    expect.objectContaining({ test_table_id: '2', value: 'initial2' })
                ])
            );
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });
});