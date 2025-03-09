const fs = require('fs');
const ndjson = require('ndjson');
const fhirpath = require('fhirpath');
const fhirpath_r4_model = require('fhirpath/fhir-context/r4');
const config = require('./config');

function logDebug(message) {
    if (config.debug) {
        console.log(message);
    }
}

const customFunctions = {
    getResourceKey: {
        fn: (inputs) => inputs.map((resource) => resource.id || null),
        arity: { 0: [] },
    },
    isActive: {
        fn: (inputs) => inputs.map((resource) => resource.active || false),
        arity: { 0: [] },
    }
};

function createConstantFunctions(constants) {
    return constants.reduce((acc, constant) => {
        acc[constant.name] = {
            fn: () => [constant.value],
            arity: { 0: [] }
        };
        return acc;
    }, {});
}

function evaluateFhirPath(resource, path, context) {
    try {
        const result = fhirpath.evaluate(
            resource,
            path,
            null,
            fhirpath_r4_model,
            context
        );
        logDebug(`Evaluated path "${path}": ${JSON.stringify(result, null, 2)}`);
        return result;
    } catch (err) {
        console.error(`Error evaluating FHIRPath "${path}" on resource:`, resource);
        console.error(err);
        return [];
    }
}

function processColumns(resource, columns, context) {
    const row = {};
    columns.forEach((col) => {
        const result = evaluateFhirPath(resource, col.path, context);
        row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
    });
    return row;
}

function evaluateWhereClauses(resource, whereClauses, context) {
    return whereClauses.every((where) => {
        const result = evaluateFhirPath(resource, where.path, context);
        return result && result.length > 0 && result[0] === true;
    });
}

function processNestedSelect(resource, nestedSelect, context) {
    const rows = [];
    let parentElements = [];

    if (nestedSelect.forEach) {
        parentElements = evaluateFhirPath(resource, nestedSelect.forEach, context);
    } else if (nestedSelect.forEachOrNull) {
        parentElements = evaluateFhirPath(resource, nestedSelect.forEachOrNull, context);
        if (parentElements.length === 0) {
            parentElements = [null];
        }
    }

    parentElements.forEach(element => {
        const row = {};

        // Process columns for this nested select
        if (nestedSelect.column) {
            nestedSelect.column.forEach(col => {
                const result = element ?
                    evaluateFhirPath(element, col.path, context) :
                    [];
                row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
            });
        }

        // Process further nested selects recursively
        if (nestedSelect.select) {
            nestedSelect.select.forEach(childSelect => {
                const childRows = processNestedSelect(element || resource, childSelect, context);
                // Combine child rows with current row
                childRows.forEach(childRow => {
                    rows.push({ ...row, ...childRow });
                });
            });
        } else {
            rows.push(row);
        }
    });

    return rows;
}

async function processNdjson(filePath, viewDefinition) {
    const { columns, whereClauses, constants, select } = viewDefinition;

    // Create context with custom functions and constants
    const context = {
        userInvocationTable: {
            ...customFunctions,
            ...createConstantFunctions(constants || [])
        }
    };

    const rows = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(ndjson.parse())
            .on('data', (resource) => {
                logDebug(`Processing resource: ${JSON.stringify(resource, null, 2)}`);

                try {
                    // Evaluate where clauses
                    if (evaluateWhereClauses(resource, whereClauses || [], context)) {
                        // Process main columns
                        const mainRow = processColumns(resource, columns, context);

                        // Process nested selects
                        if (select && select.length > 0) {
                            select.forEach(selectDef => {
                                if (selectDef.select) {
                                    const nestedRows = processNestedSelect(resource, selectDef, context);
                                    nestedRows.forEach(nestedRow => {
                                        rows.push({ ...mainRow, ...nestedRow });
                                    });
                                }
                            });
                        } else {
                            rows.push(mainRow);
                        }
                    }
                } catch (err) {
                    console.error('Error processing resource:', err);
                    console.error('Resource:', JSON.stringify(resource, null, 2));
                }
            })
            .on('end', () => {
                logDebug('Finished processing NDJSON file.');
                logDebug(`Processed ${rows.length} rows`);
                resolve(rows);
            })
            .on('error', (err) => {
                console.error('Error reading NDJSON file:', err);
                reject(err);
            });
    });
}

module.exports = {
    processNdjson,
};