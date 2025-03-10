// test/viewParser.test.js
const { parseViewDefinition } = require('../src/viewParser');

describe('ViewParser', () => {
  describe('parseViewDefinition', () => {
    test('should parse basic view definition', () => {
      const viewDefinition = {
        resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
        name: 'TestView',
        status: 'active',
        resource: 'Patient',
        select: [
          {
            column: [
              { path: 'getResourceKey()', name: 'patient_id' },
              { path: 'gender', name: 'gender' },
            ],
          },
        ],
      };

      const result = parseViewDefinition(viewDefinition);
      
      expect(result.columns).toHaveLength(2);
      expect(result.columns[0]).toEqual({
        path: 'getResourceKey()',
        name: 'patient_id',
        type: 'string',
        description: '',
        collection: false,
        tags: [],
        selectPath: '0'
      });
    });

    test('should throw error for missing required fields', () => {
      const invalidViewDefinition = {
        resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
        select: []
      };

      expect(() => parseViewDefinition(invalidViewDefinition))
        .toThrow('Invalid ViewDefinition: Missing required field');
    });
  });
});

// test/ndjsonProcessor.test.js
const { processNdjson } = require('../src/ndjsonProcessor');
const fs = require('fs');
const path = require('path');

describe('NdjsonProcessor', () => {
  const testDataPath = path.join(__dirname, 'fixtures', 'test.ndjson');

  beforeAll(() => {
    // Create test NDJSON file
    const testData = [
      JSON.stringify({ resourceType: 'Patient', id: '1', gender: 'male' }),
      JSON.stringify({ resourceType: 'Patient', id: '2', gender: 'female' })
    ].join('\n');
    
    fs.writeFileSync(testDataPath, testData);
  });

  afterAll(() => {
    // Cleanup test file
    fs.unlinkSync(testDataPath);
  });

  test('should process NDJSON file correctly', async () => {
    const options = {
      columns: [
        { path: 'id', name: 'patient_id' },
        { path: 'gender', name: 'gender' }
      ],
      resource: 'Patient'
    };

    const results = await processNdjson(testDataPath, options);
    
    expect(results).toHaveLength(2);
    expect(results[0]).toEqual({
      patient_id: '1',
      gender: 'male'
    });
  });
});

// test/duckdbHandler.test.js
const { getDatabaseHandler } = require('../src/duckdbHandler');

describe('DuckDBHandler', () => {
  let dbHandler;

  beforeAll(async () => {
    dbHandler = await getDatabaseHandler();
  });

  test('should create table and insert data', async () => {
    const tableName = 'test_table';
    const columns = [
      { name: 'id', type: 'VARCHAR' },
      { name: 'value', type: 'VARCHAR' }
    ];
    const data = [
      { id: '1', value: 'test1' },
      { id: '2', value: 'test2' }
    ];

    await dbHandler.createTable(tableName, columns);
    const result = await dbHandler.upsertData(tableName, data, 'id');

    expect(result.inserted).toBe(2);
    expect(result.errors).toBe(0);
  });
});