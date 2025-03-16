import { describe, it, expect } from '@jest/globals';
import { parseViewDefinition } from '../src/viewParser.js';

describe('ViewParser', () => {
  it('should parse basic view definition', () => {
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

  it('should throw error for missing required fields', () => {
    const invalidViewDefinition = {
      resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
      select: []
    };

    expect(() => parseViewDefinition(invalidViewDefinition))
      .toThrow('Invalid ViewDefinition: Missing required field');
  });

  it('should handle nested select statements', () => {
    const viewDefinition = {
      resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
      name: 'TestView',
      status: 'active',
      resource: 'Patient',
      select: [
        {
          forEach: 'address',
          column: [
            { path: 'city', name: 'city' },
            { path: 'state', name: 'state' }
          ],
          select: [
            {
              forEach: 'line',
              column: [
                { path: 'line', name: 'address_line' }
              ]
            }
          ]
        }
      ]
    };

    const result = parseViewDefinition(viewDefinition);
    expect(result.columns).toHaveLength(2);
    expect(result.nestedSelects).toHaveLength(1);
  });

  it('should handle union all', () => {
    const viewDefinition = {
      resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
      name: 'TestView',
      status: 'active',
      resource: 'Patient',
      select: [
        {
          unionAll: [
            {
              column: [
                { path: 'id', name: 'patient_id' }
              ]
            },
            {
              column: [
                { path: 'gender', name: 'gender' }
              ]
            }
          ]
        }
      ]
    };

    const result = parseViewDefinition(viewDefinition);
    expect(result.columns).toHaveLength(2);
  });

  it('should handle constants and custom functions', () => {
    const viewDefinition = {
      resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
      name: 'TestView',
      status: 'active',
      resource: 'Patient',
      constant: [
        { name: 'testConstant', valueString: 'testValue' }
      ],
      select: [
        {
          column: [
            { path: 'getResourceKey()', name: 'patient_id' }
          ]
        }
      ]
    };

    const result = parseViewDefinition(viewDefinition);
    expect(result.constants).toHaveLength(1);
    expect(result.constants[0]).toEqual({
      name: 'testConstant',
      value: 'testValue',
      type: 'string'
    });
  });
});