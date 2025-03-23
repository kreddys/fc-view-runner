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
            logger.error('Error resetting table:', error);
            throw error;
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    afterAll(async () => {
        const tablesToDelete = ['test_table', 'patient_identifier']; // Add all tables to delete here
        let connection;
        try {
            connection = await dbHandler.getConnection();

            // Drop each table
            for (const tableName of tablesToDelete) {
                await connection.run(`DROP TABLE IF EXISTS ${tableName}`);
                const sequenceName = `${tableName}_id_seq`;
                await connection.run(`DROP SEQUENCE IF EXISTS ${sequenceName}`);
            }
        } catch (error) {
            logger.error('Error cleaning up tables:', error);
            throw error;
        } finally {
            if (connection) {
                await dbHandler.releaseConnection(connection);
            }
        }
    });

    it('should insert data into a table', async () => {
        const tableName = 'test_table';
        const rows = [
            { test_table_id: '1', value: 'test1' },
            { test_table_id: '2', value: 'test2' }
        ];

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

    describe('Upsert Logic', () => {
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
            expect(newResult.inserted).toBe(1); // 1 record inserted (test_table_id = '3')
            expect(newResult.updated).toBe(1); // 1 record updated (test_table_id = '1')

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
});