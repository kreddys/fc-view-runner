const fs = require('fs');
const ndjson = require('ndjson');
const fhirpath = require('fhirpath');
const fhirpath_r4_model = require('fhirpath/fhir-context/r4');
const config = require('./config');

function logDebug(message) {
    if (config.debug) {
        console.log(`[DEBUG] ${new Date().toISOString()} - ${message}`);
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
    if (!columns || !Array.isArray(columns)) {
        throw new Error('Invalid columns: columns must be an array');
    }

    const row = {};
    columns.forEach((col) => {
        const result = evaluateFhirPath(resource, col.path, context);
        row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
    });

    logDebug(`Processed row: ${JSON.stringify(row, null, 2)}`);
    return row;
}

function evaluateWhereClauses(resource, whereClauses, context) {
    return whereClauses.every((where) => {
        const result = evaluateFhirPath(resource, where.path, context);
        logDebug(`Evaluated where clause "${where.path}": ${JSON.stringify(result, null, 2)}`);
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

        if (nestedSelect.column) {
            nestedSelect.column.forEach(col => {
                const result = element ?
                    evaluateFhirPath(element, col.path, context) :
                    [];
                row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
            });
        }

        if (nestedSelect.select) {
            nestedSelect.select.forEach(childSelect => {
                const childRows = processNestedSelect(element || resource, childSelect, context);
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

async function processNdjson(filePath, { columns, whereClauses, resource, constants, select }) {
    const context = {
        userInvocationTable: {
            ...customFunctions,
            ...createConstantFunctions(constants || [])
        }
    };

    const rows = [];
    let totalRecords = 0;
    let parsedRecords = 0;

    // Dynamically import p-limit
    const { default: pLimit } = await import('p-limit');
    const limit = pLimit(config.asyncProcessing ? 10 : 1); // Control concurrency

    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath)
            .pipe(ndjson.parse())
            .on('data', (resourceData) => {
                totalRecords++;
                logDebug(`Processing resource ${totalRecords}: ${resourceData.id || 'N/A'}`);

                limit(async () => {
                    try {
                        if (resourceData.resourceType !== resource) {
                            logDebug(`Skipping resource of type ${resourceData.resourceType} (expected ${resource})`);
                            return;
                        }

                        const includeResource = whereClauses
                            ? evaluateWhereClauses(resourceData, whereClauses, context)
                            : true;

                        logDebug(`Resource ${resourceData.id} included: ${includeResource}`);

                        if (includeResource) {
                            const mainRow = processColumns(resourceData, columns, context);

                            if (select && select.length > 0 && select.some(selectDef => selectDef.select)) {
                                select.forEach(selectDef => {
                                    if (selectDef.select) {
                                        const nestedRows = processNestedSelect(resourceData, selectDef, context);
                                        nestedRows.forEach(nestedRow => {
                                            rows.push({ ...mainRow, ...nestedRow });
                                        });
                                    }
                                });
                            } else {
                                rows.push(mainRow);
                                logDebug(`Added row to rows array: ${JSON.stringify(mainRow, null, 2)}`);
                            }

                            parsedRecords++;
                        }

                        // Log progress every 1000 records
                        if (totalRecords % 1000 === 0) {
                            console.log(`Processed ${totalRecords} records (${parsedRecords} parsed)`);
                        }
                    } catch (err) {
                        console.error('Error processing resource:', err);
                        console.error('Resource:', JSON.stringify(resourceData, null, 2));
                    }
                }).catch(reject);
            })
            .on('end', () => {
                limit(() => {
                    console.log(`Finished processing NDJSON file. Total records: ${totalRecords}, Parsed records: ${parsedRecords}`);
                    resolve(rows);
                });
            })
            .on('error', (err) => {
                console.error('Error reading NDJSON file:', err);
                reject(err);
            });
    });
}

module.exports = {
    processNdjson
};