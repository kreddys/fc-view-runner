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
            // inputs is an array of references
            if (!inputs || inputs.length === 0) return [];

            const reference = inputs[0];
            if (!reference.reference) return [];

            // Extract the reference ID (e.g., "Patient/123" -> "123")
            const referenceId = reference.reference.split('/')[1];

            // If a resourceType is provided, validate the reference type
            if (resourceType && !reference.reference.startsWith(resourceType)) {
                return []; // Return empty collection if the reference type doesn't match
            }

            return [referenceId]; // Return the reference ID
        },
        arity: { 0: [], 1: ['String'] }, // Optional resourceType parameter
    },
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
    if (!columns || !Array.isArray(columns)) {
        throw new Error('Invalid columns: columns must be an array');
    }

    const row = {};
    let hasData = false; // Track if any column has non-null data

    columns.forEach((col) => {
        const result = evaluateFhirPath(resource, col.path, context);
        row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;

        // If any column has non-null data, mark the row as valid
        if (row[col.name] !== null) {
            hasData = true;
        }
    });

    // Return null if the row has no data (all columns are null)
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
    // Get the resource type and create the dynamic resource key name
    const resourceType = resourceData.resourceType;
    const resourceKeyName = `${resourceType.toLowerCase()}_id`;

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
    let hasForEachData = false;
    select.forEach(selectDef => {
        if (selectDef.forEach) {
            const elements = evaluateFhirPath(resourceData, selectDef.forEach, context);
            logger.debug(`ForEach ${selectDef.forEach} returned ${elements ? elements.length : 0} elements`);

            if (elements && elements.length > 0) {
                hasForEachData = true;
                elements.forEach(element => {
                    if (element) {
                        const nestedRow = processColumns(element, selectDef.column, context);
                        if (nestedRow && Object.keys(nestedRow).length > 0) {
                            resultRows.push({
                                ...mainRow,
                                ...nestedRow
                            });
                        }
                    }
                });
            }
        }
    });

    // If we have base data but no forEach data was processed, return just the base row
    if (!hasForEachData && Object.keys(mainRow).length > 0) {
        resultRows.push(mainRow);
    }

    // Filter rows based on the dynamic resource key
    return resultRows.filter(row => row[resourceKeyName] === resourceData.id);
}

export { processNdjson };