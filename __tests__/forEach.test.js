import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js'; // Import the processNdjson function
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ES Modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('NdjsonProcessor - forEach Functionality', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'patient_address.ndjson');

    beforeAll(() => {
        // Create a test NDJSON file with a patient having multiple addresses
        const testData = [
            JSON.stringify({
                resourceType: 'Patient',
                id: '1',
                address: [
                    {
                        line: ['123 Main St'],
                        city: 'Springfield',
                        state: 'IL',
                        postalCode: '62701',
                        country: 'USA',
                        extension: [
                            {
                                url: 'http://hl7.org/fhir/StructureDefinition/geolocation',
                                extension: [
                                    { url: 'latitude', valueDecimal: 39.7817 },
                                    { url: 'longitude', valueDecimal: -89.6501 }
                                ]
                            }
                        ]
                    },
                    {
                        line: ['456 Elm St'],
                        city: 'Chicago',
                        state: 'IL',
                        postalCode: '60601',
                        country: 'USA',
                        extension: [
                            {
                                url: 'http://hl7.org/fhir/StructureDefinition/geolocation',
                                extension: [
                                    { url: 'latitude', valueDecimal: 41.8781 },
                                    { url: 'longitude', valueDecimal: -87.6298 }
                                ]
                            }
                        ]
                    }
                ]
            })
        ].join('\n');

        fs.writeFileSync(testDataPath, testData);
    });

    afterAll(() => {
        // Clean up the test file
        fs.unlinkSync(testDataPath);
    });

    it('should process multiple addresses using forEach', async () => {
        const viewDefinition = {
            resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
            name: 'Patient_Address',
            status: 'draft',
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
                        { path: 'country', name: 'country' },
                        {
                            path: "extension.where(url = 'http://hl7.org/fhir/StructureDefinition/geolocation').extension.where(url = 'latitude').valueDecimal",
                            name: 'latitude'
                        },
                        {
                            path: "extension.where(url = 'http://hl7.org/fhir/StructureDefinition/geolocation').extension.where(url = 'longitude').valueDecimal",
                            name: 'longitude'
                        }
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

        const results = await processNdjson(testDataPath, options);
        expect(results).toHaveLength(2); // Expect 2 rows (one for each address)

        // Verify the first address
        expect(results[0]).toEqual({
            patient_id: '1',
            street: '123 Main St',
            city: 'Springfield',
            state: 'IL',
            zip: '62701',
            country: 'USA',
            latitude: 39.7817,
            longitude: -89.6501
        });

        // Verify the second address
        expect(results[1]).toEqual({
            patient_id: '1',
            street: '456 Elm St',
            city: 'Chicago',
            state: 'IL',
            zip: '60601',
            country: 'USA',
            latitude: 41.8781,
            longitude: -87.6298
        });
    });
});