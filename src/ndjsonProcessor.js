import fs from 'fs';
import readline from 'readline'; // Use readline to process the file line by line
import fhirpath from 'fhirpath';
import fhirpath_r4_model from 'fhirpath/fhir-context/r4/index.js';
import config from './config.js';
import logger from './logger.js';
import { logFailedRecord } from './utils.js';

// Define custom functions for FHIRPath evaluation
const customFunctions = {
    getResourceKey: {
        fn: (inputs) => {
            // inputs is an array of resources or references
            return inputs.map((resource) => resource.id || null);
        },
        arity: { 0: [] }, // No parameters
    },
    getReferenceKey: {
        fn: (inputs, resourceType) => {
            console.log('=== START getReferenceKey DEBUG ===');
            console.log('Inputs:', JSON.stringify(inputs, null, 2));
            console.log('ResourceType parameter:', resourceType);

            try {
                // Handle empty inputs
                if (!inputs || inputs.length === 0) {
                    console.log('No inputs provided');
                    return [];
                }

                // Get the first input (FHIRPath functions work on collections)
                const input = inputs[0];
                console.log('Processing input:', JSON.stringify(input, null, 2));

                // Extract reference string
                let reference;
                if (typeof input === 'string') {
                    console.log('Input is direct reference string');
                    reference = input;
                } else if (input && typeof input === 'object') {
                    console.log('Input is object, checking for reference property');
                    if (input.reference) {
                        reference = input.reference;
                    } else if (input.reference && input.reference.reference) {
                        reference = input.reference.reference;
                    }
                }

                if (!reference) {
                    console.log('No reference found in input');
                    return [];
                }

                console.log('Extracted reference:', reference);

                // Split reference (format "ResourceType/ID")
                const parts = reference.split('/');
                console.log('Reference parts:', parts);

                if (parts.length !== 2) {
                    console.log('Invalid reference format');
                    return [];
                }

                // Validate resource type if specified
                if (resourceType && parts[0] !== resourceType) {
                    console.log(`Resource type mismatch (expected ${resourceType}, got ${parts[0]})`);
                    return [];
                }

                const result = [parts[1]];
                console.log('=== END getReferenceKey DEBUG ===');
                console.log('Returning:', result);
                return result;
            } catch (e) {
                console.error('Error in getReferenceKey:', e);
                return [];
            }
        },
        arity: { 0: [], 1: ['String'] }
    }
};

/**
 * Creates constant functions for FHIRPath evaluation.
 * @param {Array} constants - The constants to create functions for.
 * @returns {object} The constant functions.
 */
function createConstantFunctions(constants) {
    return constants.reduce((acc, constant) => {
        acc[constant.name] = {
            fn: () => [constant.value],
            arity: { 0: [] }
        };
        return acc;
    }, {});
}

/**
 * Evaluates a FHIRPath expression on a resource.
 * @param {object} resource - The FHIR resource.
 * @param {string} path - The FHIRPath expression.
 * @param {object} context - The evaluation context.
 * @returns {Array} The result of the evaluation.
 */
function evaluateFhirPath(resource, path, context) {
    try {
        const result = fhirpath.evaluate(
            resource,
            path,
            null,
            fhirpath_r4_model,
            context
        );
        logger.debug(`Evaluated path "${path}": ${JSON.stringify(result, null, 2)}`);
        return result;
    } catch (err) {
        logger.error(`Error evaluating FHIRPath "${path}" on resource:`, resource);
        logger.error(err);
        return [];
    }
}

/**
 * Processes columns for a FHIR resource.
 * @param {object} resource - The FHIR resource.
 * @param {Array} columns - The columns to process.
 * @param {object} context - The evaluation context.
 * @returns {object|null} The processed row or null if no valid data is found.
 */
function processColumns(resource, columns, context) {
    const row = {};
    let hasData = false;

    columns.forEach(col => {
        try {
            let result;
            if (col.path === "$this") {
                // Directly use the value of $this from the context
                result = [context.$this];
            } else {
                // Evaluate the FHIRPath expression for other columns
                result = evaluateFhirPath(resource, col.path, context);
            }

            logger.debug(`Evaluated path "${col.path}": ${JSON.stringify(result)}`);
            row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;

            if (row[col.name] !== null) {
                hasData = true;
            }
        } catch (error) {
            logger.error(`Error evaluating FHIRPath "${col.path}" on resource:`, resource);
            logger.error(error);
            row[col.name] = null; // Set the column value to null if evaluation fails
        }
    });

    return hasData ? row : null;
}

/**
 * Evaluates where clauses for a FHIR resource.
 * @param {object} resource - The FHIR resource.
 * @param {Array} whereClauses - The where clauses to evaluate.
 * @param {object} context - The evaluation context.
 * @returns {boolean} True if all where clauses are satisfied, false otherwise.
 */
function evaluateWhereClauses(resource, whereClauses, context) {
    return whereClauses.every((where) => {
        const result = evaluateFhirPath(resource, where.path, context);
        logger.debug(`Evaluated where clause "${where.path}": ${JSON.stringify(result, null, 2)}`);
        return result && result.length > 0 && result[0] === true;
    });
}

/**
 * Processes an NDJSON file and extracts rows based on the provided configuration.
 * @param {string} filePath - The path to the NDJSON file.
 * @param {object} options - The processing options.
 * @param {Array} options.columns - The columns to extract.
 * @param {Array} options.whereClauses - The where clauses to filter resources.
 * @param {string} options.resource - The expected resource type.
 * @param {Array} options.constants - Constants for FHIRPath evaluation.
 * @param {Array} options.select - The select definitions.
 * @returns {Promise<Array>} The processed rows.
 */
async function processNdjson(filePath, { columns, whereClauses, resource, constants, select }) {
    logger.debug(`Starting processNdjson for resource: ${resource}`);
    logger.debug(`Configuration:`, {
        columns: JSON.stringify(columns),
        whereClauses: JSON.stringify(whereClauses),
        constants: JSON.stringify(constants),
        select: JSON.stringify(select)
    });

    const context = {
        userInvocationTable: {
            ...customFunctions,
            ...createConstantFunctions(constants || [])
        }
    };

    const rows = [];
    let totalRecords = 0;
    let parsedRecords = 0;
    let invalidRecords = 0;

    const startTime = Date.now();
    const { default: pLimit } = await import('p-limit');
    const limit = pLimit(config.asyncProcessing ? config.concurrencyLimit : 1);

    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const rl = readline.createInterface({ input: stream });

        rl.on('line', (line) => {
            totalRecords++;
            logger.debug(`Processing record #${totalRecords}`);

            limit(async () => {
                try {
                    const resourceData = JSON.parse(line);
                    logger.debug(`Parsed resource data for ID: ${resourceData.id}`);

                    if (typeof resourceData !== 'object' || resourceData === null) {
                        throw new Error('Invalid resource data: not a valid JSON object');
                    }

                    if (resourceData.resourceType !== resource) {
                        logger.debug(`Skipping resource of type ${resourceData.resourceType} (expected ${resource})`);
                        return;
                    }

                    const includeResource = whereClauses
                        ? evaluateWhereClauses(resourceData, whereClauses, context)
                        : true;

                    if (includeResource) {
                        const processedRows = processResource(resourceData, {
                            columns,
                            select,
                            context
                        });

                        if (processedRows && processedRows.length > 0) {
                            rows.push(...processedRows);
                        }
                        parsedRecords++;
                    }
                } catch (err) {
                    invalidRecords++;
                    logger.error(`Error processing resource ${totalRecords}:`, err.message);
                    logFailedRecord(resource, { raw: line }, err);
                }
            }).catch(reject);
        });

        rl.on('close', () => {
            limit(() => {
                const elapsedTime = (Date.now() - startTime) / 1000;
                logger.info(`Final processing results:`, {
                    totalRecords,
                    parsedRecords,
                    invalidRecords,
                    totalTime: `${elapsedTime.toFixed(2)}s`,
                    rowsGenerated: rows.length
                });
                resolve(rows);
            });
        });

        rl.on('error', (err) => {
            logger.error('Error reading NDJSON file:', err);
            reject(err);
        });
    });
}

function processResource(resourceData, { columns, select, context }) {
    // Process base columns first
    const baseRow = columns ? processColumns(resourceData, columns, context) : {};
    const resultRows = [];

    // If no select definitions, return base row if it has data
    if (!select || select.length === 0) {
        return Object.keys(baseRow).length > 0 ? [baseRow] : [];
    }

    // Process non-forEach columns to build the base row
    let mainRow = { ...baseRow };
    select.forEach(selectDef => {
        if (!selectDef.forEach && selectDef.column) {
            const regularRow = processColumns(resourceData, selectDef.column, context);
            if (regularRow) {
                mainRow = { ...mainRow, ...regularRow };
            }
        }
    });

    // Process forEach sections
    select.forEach(selectDef => {
        if (selectDef.forEach) {
            const elements = evaluateFhirPath(resourceData, selectDef.forEach, context);
            logger.debug(`ForEach ${selectDef.forEach} returned ${elements ? elements.length : 0} elements`);

            if (elements && elements.length > 0) {
                elements.forEach(element => {
                    if (element) {
                        // Set $this to the current element in the loop
                        const forEachContext = { ...context, $this: element };
                        logger.debug(`Current element in forEach loop: ${JSON.stringify(element)}`);
                        logger.debug(`Updated context with $this: ${JSON.stringify(forEachContext)}`);

                        // Process columns with the updated context
                        const nestedRow = processColumns(element, selectDef.column, forEachContext);
                        if (nestedRow && Object.keys(nestedRow).length > 0) {
                            resultRows.push({
                                ...mainRow,
                                ...nestedRow
                            });
                        } else {
                            logger.debug(`No data found for element: ${JSON.stringify(element)}`);
                        }
                    }
                });
            } else {
                logger.debug(`No elements found for forEach path: ${selectDef.forEach}`);
            }
        }
    });

    // If we have no forEach data but have base data, return just the base row
    if (resultRows.length === 0 && Object.keys(mainRow).length > 0) {
        resultRows.push(mainRow);
    }

    // Return only rows for this specific resource
    return resultRows;
}

export { processNdjson };