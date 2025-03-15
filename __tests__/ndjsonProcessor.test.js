import { jest, describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { processNdjson } from '../src/ndjsonProcessor.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url'; // Add this import

// Get __dirname equivalent in ES Modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename); // Use path.dirname

// Mock p-limit
jest.mock('p-limit', () => {
  return () => (fn) => fn();
});

describe('NdjsonProcessor', () => {
  const testDataPath = path.join(__dirname, 'fixtures', 'test.ndjson');

  beforeAll(() => {
    // Create test NDJSON file
    const testData = [
      JSON.stringify({ resourceType: 'Patient', id: '1', gender: 'male', active: true }),
      JSON.stringify({ resourceType: 'Patient', id: '2', gender: 'female', active: false }),
      JSON.stringify({ resourceType: 'Patient', id: '3', gender: 'other', active: true })
    ].join('\n');

    if (!fs.existsSync(path.dirname(testDataPath))) {
      fs.mkdirSync(path.dirname(testDataPath), { recursive: true });
    }
    fs.writeFileSync(testDataPath, testData);
  });

  afterAll(() => {
    fs.unlinkSync(testDataPath);
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

  it('should filter resources using where clause', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient',
      whereClauses: [
        { path: 'active' }
      ]
    };

    const results = await processNdjson(testDataPath, options);
    expect(results).toHaveLength(2);
    expect(results.map(r => r.patient_id)).toEqual(['1', '3']);
  });

  it('should handle invalid JSON lines', async () => {
    const invalidTestPath = path.join(__dirname, 'fixtures', 'invalid.ndjson');
    const invalidData = [
      JSON.stringify({ resourceType: 'Patient', id: '1' }),
      'invalid json',
      JSON.stringify({ resourceType: 'Patient', id: '2' })
    ].join('\n');

    fs.writeFileSync(invalidTestPath, invalidData);

    const options = {
      columns: [{ path: 'id', name: 'patient_id' }],
      resource: 'Patient'
    };

    const results = await processNdjson(invalidTestPath, options);
    expect(results).toHaveLength(2);

    fs.unlinkSync(invalidTestPath);
  });
});