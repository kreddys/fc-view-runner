const { parseViewDefinition } = require('../src/viewParser');

const viewDefinition = {
    resourceType: 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition',
    select: [
        {
            column: [
                { path: 'getResourceKey()', name: 'patient_id' },
                { path: 'gender', name: 'gender' },
            ],
        },
    ],
};

const columns = parseViewDefinition(viewDefinition);
console.assert(columns.length === 2, 'Test parseViewDefinition failed');
console.assert(columns[0].path === 'getResourceKey()', 'Test parseViewDefinition failed');
console.assert(columns[1].name === 'gender', 'Test parseViewDefinition failed');

console.log('All tests passed!');