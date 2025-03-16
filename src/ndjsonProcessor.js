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

    logger.debug(`Initialized with concurrency limit: ${config.asyncProcessing ? config.concurrencyLimit : 1}`);

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

                    logger.debug(`Resource ${resourceData.id} included: ${includeResource}`);

                    if (includeResource) {
                        const mainRow = processColumns(resourceData, columns, context);
                        logger.debug(`Main row data:`, mainRow);

                        if (select && select.length > 0) {
                            logger.debug(`Processing ${select.length} select definitions`);
                            let combinedRow = mainRow || {};

                            select.forEach((selectDef, selectIndex) => {
                                logger.debug(`Processing select definition #${selectIndex + 1}:`, selectDef);

                                if (selectDef.forEach) {
                                    const elements = evaluateFhirPath(resourceData, selectDef.forEach, context);
                                    logger.debug(`ForEach ${selectDef.forEach} returned ${elements.length} elements`);

                                    elements.forEach((element, elementIndex) => {
                                        logger.debug(`Processing forEach element #${elementIndex + 1}:`, element);

                                        const nestedRow = processColumns(element, selectDef.column, context);
                                        logger.debug(`Nested row data:`, nestedRow);

                                        if (nestedRow) {
                                            // Combine with the existing combined row
                                            combinedRow = { ...combinedRow, ...nestedRow };
                                            logger.debug(`Updated combined row:`, combinedRow);
                                        }
                                    });
                                } else if (selectDef.column) {
                                    logger.debug(`Processing regular columns`);
                                    const regularRow = processColumns(resourceData, selectDef.column, context);

                                    if (regularRow) {
                                        // Combine with the existing combined row
                                        combinedRow = { ...combinedRow, ...regularRow };
                                        logger.debug(`Updated combined row with regular columns:`, combinedRow);
                                    }
                                }
                            });

                            // Only push the combined row once after all processing is complete
                            if (Object.keys(combinedRow).length > 0) {
                                logger.debug(`Adding final combined row:`, combinedRow);
                                rows.push(combinedRow);
                            }
                        } else if (mainRow) {
                            logger.debug(`Adding main row only:`, mainRow);
                            rows.push(mainRow);
                        }

                        parsedRecords++;
                    }
                } catch (err) {
                    invalidRecords++;
                    logger.error(`Error processing resource ${totalRecords}:`, err.message);
                    logger.error('Invalid resource data:', line);
                    logFailedRecord(resource, { raw: line }, err);
                }

                if (totalRecords % 1000 === 0) {
                    const elapsedTime = (Date.now() - startTime) / 1000;
                    const recordsPerSecond = totalRecords / elapsedTime;
                    const estimatedTotalTime = (totalRecords / recordsPerSecond).toFixed(2);
                    const estimatedTimeRemaining = (estimatedTotalTime - elapsedTime).toFixed(2);

                    logger.info(`Progress stats:`, {
                        totalRecords,
                        parsedRecords,
                        invalidRecords,
                        elapsedTime: `${elapsedTime.toFixed(2)}s`,
                        estimatedRemaining: `${estimatedTimeRemaining}s`
                    });
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
                logger.debug(`Final rows:`, rows);
                resolve(rows);
            });
        });

        rl.on('error', (err) => {
            logger.error('Error reading NDJSON file:', err);
            reject(err);
        });
    });
}

export { processNdjson };