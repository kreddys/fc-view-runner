import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js'; // Import the processNdjson function
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ES Modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Add to forEach.test.js

describe('NdjsonProcessor - Advanced forEach Scenarios', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'patient_complex.ndjson');

    beforeAll(() => {
        // Create test data with various scenarios
        const testData = [
            // Scenario 1: Patient with no addresses
            JSON.stringify({
                resourceType: 'Patient',
                id: '1',
                active: true,
                gender: 'male'
            }),
            // Scenario 2: Patient with empty address array
            JSON.stringify({
                resourceType: 'Patient',
                id: '2',
                active: true,
                gender: 'female',
                address: []
            }),
            // Scenario 3: Patient with multiple addresses, some having missing fields
            JSON.stringify({
                resourceType: 'Patient',
                id: '3',
                active: true,
                gender: 'male',
                address: [
                    {
                        line: ['123 Main St'],
                        city: 'Springfield',
                        state: 'IL'
                        // Missing postalCode and country
                    },
                    {
                        // Missing line
                        city: 'Chicago',
                        state: 'IL',
                        postalCode: '60601',
                        country: 'USA'
                    }
                ]
            }),
            // Scenario 4: Patient with nested arrays in forEach
            JSON.stringify({
                resourceType: 'Patient',
                id: '4',
                active: true,
                contact: [
                    {
                        relationship: [
                            { text: 'Emergency Contact' }
                        ],
                        address: {
                            line: ['789 Oak St'],
                            city: 'Boston',
                            state: 'MA',
                            postalCode: '02108'
                        },
                        telecom: [
                            { system: 'phone', value: '555-0123' },
                            { system: 'email', value: 'contact@email.com' }
                        ]
                    }
                ]
            })
        ].join('\n');

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

    it('should handle patient with empty address array', async () => {
        const viewDefinition = {
            resource: 'Patient',
            select: [
                {
                    column: [
                        { path: 'getResourceKey()', name: 'patient_id' },
                        { path: 'gender', name: 'gender' }
                    ]
                },
                {
                    forEach: 'address',
                    column: [
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

        expect(results).toHaveLength(1);
        expect(results[0]).toEqual({
            patient_id: '2',
            gender: 'female'
        });
    });

    it('should handle missing fields in forEach elements', async () => {
        const viewDefinition = {
            resource: 'Patient',
            select: [
                {
                    column: [
                        { path: 'getResourceKey()', name: 'patient_id' }
                    ]
                },
                {
                    forEach: 'address',
                    column: [
                        { path: "line.join('\\n')", name: 'street' },
                        { path: 'city', name: 'city' },
                        { path: 'postalCode', name: 'zip' }
                    ]
                }
            ]
        };

        const results = await processNdjson(testDataPath, {
            columns: viewDefinition.select.flatMap(s => s.column),
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual({
            patient_id: '3',
            street: '123 Main St',
            city: 'Springfield',
            zip: null
        });
        expect(results[1]).toEqual({
            patient_id: '3',
            street: null,
            city: 'Chicago',
            zip: '60601'
        });
    });

    it('should handle nested arrays in forEach', async () => {
        const viewDefinition = {
            resource: 'Patient',
            select: [
                {
                    column: [
                        { path: 'getResourceKey()', name: 'patient_id' }
                    ]
                },
                {
                    forEach: 'contact',
                    column: [
                        { path: 'relationship.text', name: 'relationship' },
                        { path: 'address.city', name: 'contact_city' },
                        { path: "telecom.where(system='phone').value", name: 'phone' },
                        { path: "telecom.where(system='email').value", name: 'email' }
                    ]
                }
            ]
        };

        const results = await processNdjson(testDataPath, {
            columns: viewDefinition.select.flatMap(s => s.column),
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        expect(results).toHaveLength(1);
        expect(results[0]).toEqual({
            patient_id: '4',
            relationship: 'Emergency Contact',
            contact_city: 'Boston',
            phone: '555-0123',
            email: 'contact@email.com'
        });
    });
});