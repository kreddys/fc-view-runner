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

describe('NdjsonProcessor', () => {
  const testDataPath = path.join(__dirname, 'fixtures', 'test.ndjson');
  const emptyDataPath = path.join(__dirname, 'fixtures', 'empty.ndjson');
  const invalidDataPath = path.join(__dirname, 'fixtures', 'invalid.ndjson');
  const largeDataPath = path.join(__dirname, 'fixtures', 'large.ndjson');
  const referenceDataPath = path.join(__dirname, 'fixtures', 'reference.ndjson');
  const extensionDataPath = path.join(__dirname, 'fixtures', 'extension.ndjson');

  beforeAll(() => {
    // Create test NDJSON files
    const testData = [
      JSON.stringify({ resourceType: 'Patient', id: '1', gender: 'male', active: true }),
      JSON.stringify({ resourceType: 'Patient', id: '2', gender: 'female', active: false }),
      JSON.stringify({ resourceType: 'Patient', id: '3', gender: 'other', active: true })
    ].join('\n');

    const largeData = Array.from({ length: 1000 }, (_, i) =>
      JSON.stringify({ resourceType: 'Patient', id: `${i + 1}`, gender: 'male', active: true })
    ).join('\n');

    // Test data for getReferenceKey
    const referenceData = [
      JSON.stringify({ resourceType: 'Patient', id: '1', gender: 'male', active: true }),
      JSON.stringify({ resourceType: 'Observation', id: '1', status: 'final', subject: { reference: 'Patient/1' }, valueQuantity: { value: 120 } }),
      JSON.stringify({ resourceType: 'Observation', id: '2', status: 'final', subject: { reference: 'Patient/2' }, valueQuantity: { value: 80 } }),
      JSON.stringify({ resourceType: 'Observation', id: '3', status: 'final', subject: {}, valueQuantity: { value: 98.6 } }) // Missing reference
    ].join('\n');

    // Test data for extensions
    const extensionData = [
      JSON.stringify({
        resourceType: 'Patient',
        id: '1238',
        meta: { /* ... */ },
        text: { /* ... */ },
        extension: [
          {
            url: 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
            extension: [
              {
                url: 'ombCategory',
                valueCoding: {
                  system: 'urn:oid:2.16.840.1.113883.6.238',
                  code: '2106-3',
                  display: 'White'
                }
              },
              {
                url: 'text',
                valueString: 'White'
              }
            ]
          },
          {
            url: 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity',
            extension: [
              {
                url: 'ombCategory',
                valueCoding: {
                  system: 'urn:oid:2.16.840.1.113883.6.238',
                  code: '2186-5',
                  display: 'Not Hispanic or Latino'
                }
              },
              {
                url: 'text',
                valueString: 'Not Hispanic or Latino'
              }
            ]
          },
          {
            url: 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName',
            valueString: 'Freida957 O\'Reilly797'
          },
          {
            url: 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex',
            valueCode: 'M'
          },
          {
            url: 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace',
            valueAddress: {
              city: 'Memphis',
              state: 'Tennessee',
              country: 'US'
            }
          }
        ],
        identifier: [ /* ... */],
        name: [ /* ... */],
        telecom: [ /* ... */],
        gender: 'male',
        birthDate: '1970-07-09',
        address: [ /* ... */],
        maritalStatus: { /* ... */ },
        multipleBirthBoolean: false,
        communication: [ /* ... */]
      })
    ].join('\n');

    fs.writeFileSync(testDataPath, testData);
    fs.writeFileSync(emptyDataPath, '');
    fs.writeFileSync(invalidDataPath, 'invalid json\n' + testData);
    fs.writeFileSync(largeDataPath, largeData);
    fs.writeFileSync(referenceDataPath, referenceData);
    fs.writeFileSync(extensionDataPath, extensionData);
  });

  afterAll(() => {
    // Clean up test files
    fs.unlinkSync(testDataPath);
    fs.unlinkSync(emptyDataPath);
    fs.unlinkSync(invalidDataPath);
    fs.unlinkSync(largeDataPath);
    fs.unlinkSync(referenceDataPath);
    fs.unlinkSync(extensionDataPath);
  });

  it('should process NDJSON file with basic columns', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(testDataPath, options);
    expect(results).toHaveLength(3);
    expect(results[0]).toEqual({
      patient_id: '1',
      gender: 'male'
    });
  });

  it('should handle an empty NDJSON file', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(emptyDataPath, options);
    expect(results).toHaveLength(0);
  });

  it('should handle invalid JSON lines', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(invalidDataPath, options);
    expect(results).toHaveLength(3); // Only valid lines should be processed
  });

  it('should handle large NDJSON files', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(largeDataPath, options);
    expect(results).toHaveLength(1000);
  });

  it('should respect concurrency limit', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient',
      asyncProcessing: true,
      concurrencyLimit: 2
    };

    const results = await processNdjson(testDataPath, options);
    expect(results).toHaveLength(3);
  });

  it('should extract resource keys using getResourceKey', async () => {
    const options = {
      columns: [
        { path: 'getResourceKey()', name: 'resource_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(testDataPath, options);
    expect(results).toHaveLength(3);
    expect(results[0]).toEqual({
      resource_id: '1',
      gender: 'male'
    });
  });

  it('should extract reference keys using getReferenceKey', async () => {
    const options = {
      columns: [
        { path: 'getResourceKey()', name: 'observation_id' },
        { path: "subject.getReferenceKey('Patient')", name: 'patient_id' }, // Use single quotes
        { path: 'valueQuantity.value', name: 'value' }
      ],
      resource: 'Observation'
    };

    const results = await processNdjson(referenceDataPath, options);
    expect(results).toHaveLength(3);

    // Check the first observation
    expect(results[0]).toEqual({
      observation_id: '1',
      patient_id: '1',
      value: 120
    });

    // Check the second observation
    expect(results[1]).toEqual({
      observation_id: '2',
      patient_id: '2',
      value: 80
    });

    // Check the third observation (missing reference)
    expect(results[2]).toEqual({
      observation_id: '3',
      patient_id: null, // Missing reference should return null
      value: 98.6
    });
  });

  it('should extract extension data into views', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' },
        { path: "extension.where(url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').extension.where(url = 'text').valueString", name: 'race' },
        { path: "extension.where(url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity').extension.where(url = 'ombCategory').valueCoding.display", name: 'ethnicity' },
        { path: "extension.where(url = 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName').valueString", name: 'mothers_maiden_name' },
        { path: "extension.where(url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex').valueCode", name: 'birth_sex' },
        { path: "extension.where(url = 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.city", name: 'birth_city' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(extensionDataPath, options);
    expect(results).toHaveLength(1);

    // Check the extracted extension data
    expect(results[0]).toEqual({
      patient_id: '1238',
      gender: 'male',
      race: 'White',
      ethnicity: 'Not Hispanic or Latino',
      mothers_maiden_name: 'Freida957 O\'Reilly797',
      birth_sex: 'M',
      birth_city: 'Memphis'
    });
  });
});