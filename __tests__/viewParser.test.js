import { parseViewDefinition } from '../src/viewParser.js';

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