import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('NdjsonProcessor - Simple forEach Scenario', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'patient_simple.ndjson');

    beforeAll(() => {
        // Create test data with just one scenario
        const testData = JSON.stringify({
            resourceType: 'Patient',
            id: '1',
            active: true,
            gender: 'male'
        });

        fs.writeFileSync(testDataPath, testData);
    });

    afterAll(() => {
        fs.unlinkSync(testDataPath);
    });

    it('should handle patient with no addresses', async () => {
        const viewDefinition = {
            resource: 'Patient',
            select: [
                {
                    column: [
                        { path: 'getResourceKey()', name: 'patient_id' },
                        { path: 'active', name: 'active' },
                        { path: 'gender', name: 'gender' }
                    ]
                },
                {
                    forEach: 'address',
                    column: [
                        { path: "line.join('\\n')", name: 'street' },
                        { path: 'city', name: 'city' }
                    ]
                }
            ]
        };

        const results = await processNdjson(testDataPath, {
            columns: viewDefinition.select.flatMap(s => s.column),
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        // Should return one row with only patient data, no address
        expect(results).toHaveLength(1);
        expect(results[0]).toEqual({
            patient_id: '1',
            active: true,
            gender: 'male'
        });
    });
});