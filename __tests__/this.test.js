import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ES Modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Mock p-limit
jest.mock('p-limit', () => {
    return () => (fn) => fn();
});

describe('NdjsonProcessor - forEach with Multiple Categories', () => {
    const testDataPath = path.join(__dirname, 'fixtures', 'allergyintolerance_multiple_categories.ndjson');

    beforeAll(() => {
        // Create test NDJSON file with AllergyIntolerance resources
        const testData = [
            JSON.stringify({
                resourceType: 'AllergyIntolerance',
                id: '3314',
                category: ['environment', 'medication'],
                clinicalStatus: {
                    coding: [
                        {
                            system: 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical',
                            code: 'active'
                        }
                    ]
                },
                verificationStatus: {
                    coding: [
                        {
                            system: 'http://terminology.hl7.org/CodeSystem/allergyintolerance-verification',
                            code: 'confirmed'
                        }
                    ]
                },
                type: 'allergy',
                criticality: 'low',
                code: {
                    coding: [
                        {
                            system: 'http://snomed.info/sct',
                            code: '419199007',
                            display: 'Allergy to substance (finding)'
                        }
                    ],
                    text: 'Allergy to substance (finding)'
                },
                patient: {
                    reference: 'Patient/3305'
                },
                recordedDate: '1958-08-16T04:02:52-05:00'
            }),
            JSON.stringify({
                resourceType: 'AllergyIntolerance',
                id: '3315',
                category: ['food', 'biologic'],
                clinicalStatus: {
                    coding: [
                        {
                            system: 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical',
                            code: 'active'
                        }
                    ]
                },
                verificationStatus: {
                    coding: [
                        {
                            system: 'http://terminology.hl7.org/CodeSystem/allergyintolerance-verification',
                            code: 'confirmed'
                        }
                    ]
                },
                type: 'allergy',
                criticality: 'low',
                code: {
                    coding: [
                        {
                            system: 'http://snomed.info/sct',
                            code: '735029006',
                            display: 'Shellfish (substance)'
                        }
                    ],
                    text: 'Shellfish (substance)'
                },
                patient: {
                    reference: 'Patient/3305'
                },
                recordedDate: '1958-08-16T04:02:52-05:00',
                reaction: [
                    {
                        manifestation: [
                            {
                                coding: [
                                    {
                                        system: 'http://snomed.info/sct',
                                        code: '247472004',
                                        display: 'Wheal (finding)'
                                    }
                                ],
                                text: 'Wheal (finding)'
                            }
                        ],
                        severity: 'mild'
                    }
                ]
            })
        ].join('\n');

        fs.writeFileSync(testDataPath, testData);
    });

    afterAll(() => {
        // Clean up test file
        fs.unlinkSync(testDataPath);
    });

    it('should handle forEach with multiple categories correctly', async () => {
        const viewDefinition = {
            resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
            name: 'AllergyIntolerance_Category',
            status: 'draft',
            resource: 'AllergyIntolerance',
            select: [
                {
                    column: [
                        {
                            path: 'getResourceKey()',
                            name: 'allergyintolerance_id',
                            type: 'string'
                        }
                    ]
                },
                {
                    column: [
                        {
                            path: '$this',
                            name: 'category',
                            type: 'string'
                        }
                    ],
                    forEach: 'category'
                }
            ]
        };

        const results = await processNdjson(testDataPath, {
            columns: viewDefinition.select[0].column,
            resource: viewDefinition.resource,
            select: viewDefinition.select
        });

        console.log('Results:', JSON.stringify(results, null, 2));

        // Validate the results
        expect(results).toHaveLength(4); // 2 resources × 2 categories each
        expect(results).toEqual([
            {
                allergyintolerance_id: '3314',
                category: 'environment'
            },
            {
                allergyintolerance_id: '3314',
                category: 'medication'
            },
            {
                allergyintolerance_id: '3315',
                category: 'food'
            },
            {
                allergyintolerance_id: '3315',
                category: 'biologic'
            }
        ]);
    });
});