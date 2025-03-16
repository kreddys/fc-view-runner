import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('NdjsonProcessor - Simple forEach Scenario', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'patient_simple.ndjson');

    afterAll(() => {
        fs.unlinkSync(testDataPath);
    });

    it('should handle patient with single address', async () => {
        // Create test data with patient having one address
        const testData = JSON.stringify({
            resourceType: 'Patient',
            id: '1',
            active: true,
            gender: 'male',
            address: [
                {
                    line: ['123 Main St'],
                    city: 'Springfield'
                }
            ]
        });
        fs.writeFileSync(testDataPath, testData);

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
            columns: viewDefinition.select[0].column,
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        console.log('Single Address Results:', JSON.stringify(results, null, 2));
        expect(results).toHaveLength(1);
        expect(results[0]).toEqual({
            patient_id: '1',
            active: true,
            gender: 'male',
            street: '123 Main St',
            city: 'Springfield'
        });
    });

    it('should handle patient with multiple addresses', async () => {
        // Create test data with patient having two addresses
        const testData = JSON.stringify({
            resourceType: 'Patient',
            id: '1',
            active: true,
            gender: 'male',
            address: [
                {
                    line: ['123 Main St'],
                    city: 'Springfield'
                },
                {
                    line: ['456 Oak Ave'],
                    city: 'Shelbyville'
                }
            ]
        });
        fs.writeFileSync(testDataPath, testData);

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
            columns: viewDefinition.select[0].column,
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        console.log('Multiple Addresses Results:', JSON.stringify(results, null, 2));
        expect(results).toHaveLength(2);
        expect(results).toEqual([
            {
                patient_id: '1',
                active: true,
                gender: 'male',
                street: '123 Main St',
                city: 'Springfield'
            },
            {
                patient_id: '1',
                active: true,
                gender: 'male',
                street: '456 Oak Ave',
                city: 'Shelbyville'
            }
        ]);
    });
});