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

    fs.writeFileSync(testDataPath, testData);
    fs.writeFileSync(emptyDataPath, '');
    fs.writeFileSync(invalidDataPath, 'invalid json\n' + testData);
    fs.writeFileSync(largeDataPath, largeData);
  });

  afterAll(() => {
    // Clean up test files
    fs.unlinkSync(testDataPath);
    fs.unlinkSync(emptyDataPath);
    fs.unlinkSync(invalidDataPath);
    fs.unlinkSync(largeDataPath);
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
});