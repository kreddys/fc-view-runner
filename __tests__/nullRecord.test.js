import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('NdjsonProcessor - Null Record Handling', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'patient_null_test.ndjson');

    beforeAll(() => {
        // Create test data with a patient having one address
        const testData = JSON.stringify({
            resourceType: 'Patient',
            id: '1',
            address: [
                {
                    line: ['123 Main St'],
                    city: 'Springfield',
                    state: 'IL',
                    postalCode: '62701',
                    country: 'USA'
                }
            ]
        });

        fs.writeFileSync(testDataPath, testData);
    });

    afterAll(() => {
        fs.unlinkSync(testDataPath);
    });

    it('should not generate null records when processing nested data', async () => {
        const viewDefinition = {
            resource: 'Patient',
            select: [
                {
                    column: [
                        { path: 'getResourceKey()', name: 'patient_id' }
                    ]
                },
                {
                    column: [
                        { path: "line.join('\\n')", name: 'street' },
                        { path: 'city', name: 'city' },
                        { path: 'state', name: 'state' },
                        { path: 'postalCode', name: 'zip' },
                        { path: 'country', name: 'country' }
                    ],
                    forEach: 'address'
                }
            ]
        };

        const options = {
            columns: viewDefinition.select.flatMap(s => s.column),
            resource: viewDefinition.resource,
            select: viewDefinition.select
        };

        // Enable debug logging for this test
        const results = await processNdjson(testDataPath, options);

        console.log('Generated Results:', JSON.stringify(results, null, 2));

        // Verify no null records
        expect(results.every(row => Object.values(row).some(val => val !== null))).toBe(true);

        // Verify expected length
        expect(results).toHaveLength(1);

        // Verify structure
        expect(results[0]).toEqual({
            patient_id: '1',
            street: '123 Main St',
            city: 'Springfield',
            state: 'IL',
            zip: '62701',
            country: 'USA'
        });
    });
});