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

describe('NdjsonProcessor - CareTeam with ManagingOrganization', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'careteam_organization.ndjson');

    afterAll(() => {
        // Clean up test file if it exists
        if (fs.existsSync(testDataPath)) {
            fs.unlinkSync(testDataPath);
        }
    });

    it('should extract organization reference from CareTeam', async () => {
        // Create test data with CareTeam having managingOrganization
        const testData = JSON.stringify({
            "resourceType": "CareTeam",
            "id": "1279",
            "meta": {
                "versionId": "1",
                "lastUpdated": "2025-02-16T23:19:36.722+00:00",
                "source": "#xweGFFrP6vpNiP2Q",
                "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-careteam"]
            },
            "status": "active",
            "subject": {
                "reference": "Patient/1238"
            },
            "encounter": {
                "reference": "Encounter/1278"
            },
            "period": {
                "start": "2014-04-03T00:10:42-05:00"
            },
            "managingOrganization": [{
                "reference": "Organization/1193",
                "display": "VANDERBILT BEDFORD HOSPITAL LLC"
            }]
        });
        fs.writeFileSync(testDataPath, testData);

        const viewDefinition = {
            resource: "CareTeam",
            select: [
                {
                    column: [
                        { path: "getResourceKey()", name: "careteam_id", type: "string" }
                    ]
                },
                {
                    column: [
                        {
                            path: "reference.getReferenceKey('Organization')",
                            name: "managing_organization_id",
                            type: "string"
                        },
                        {
                            path: "display",
                            name: "managing_organization_display",
                            type: "string"
                        }
                    ],
                    forEach: "managingOrganization"
                }
            ]
        };

        const results = await processNdjson(testDataPath, {
            columns: viewDefinition.select[0].column,
            resource: viewDefinition.resource,
            select: viewDefinition.select,
            constants: []
        });

        console.log('CareTeam Results:', JSON.stringify(results, null, 2));

        expect(results).toHaveLength(1);
        expect(results[0]).toEqual({
            careteam_id: "1279",
            managing_organization_id: "1193",  // This is the key assertion
            managing_organization_display: "VANDERBILT BEDFORD HOSPITAL LLC"
        });
    });
});