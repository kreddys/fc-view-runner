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
        fn: (inputs) => inputs.map((resource) => resource.id || null),
        arity: { 0: [] },
    },
    isActive: {
        fn: (inputs) => inputs.map((resource) => resource.active || false),
        arity: { 0: [] },
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
 * @returns {object} The processed row.
 */
function processColumns(resource, columns, context) {
    if (!columns || !Array.isArray(columns)) {
        throw new Error('Invalid columns: columns must be an array');
    }

    const row = {};
    columns.forEach((col) => {
        const result = evaluateFhirPath(resource, col.path, context);
        row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
    });

    logger.debug(`Processed row: ${JSON.stringify(row, null, 2)}`);
    return row;
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
 * Processes nested select statements for a FHIR resource.
 * @param {object} resource - The FHIR resource.
 * @param {object} nestedSelect - The nested select definition.
 * @param {object} context - The evaluation context.
 * @returns {Array} The processed rows.
 */
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

    // Track start time for time estimation
    const startTime = Date.now();

    // Dynamically import p-limit
    const { default: pLimit } = await import('p-limit');
    const limit = pLimit(config.asyncProcessing ? config.concurrencyLimit : 1); // Control concurrency

    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const rl = readline.createInterface({ input: stream });

        rl.on('line', (line) => {
            totalRecords++;
            //logger.info(`Processing resource ${totalRecords}`); // Log progress in real-time

            limit(async () => {
                try {
                    // Parse the line as JSON
                    const resourceData = JSON.parse(line);

                    // Validate the resource data
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
                            logger.debug(`Added row to rows array: ${JSON.stringify(mainRow, null, 2)}`);
                        }

                        parsedRecords++;
                    }
                } catch (err) {
                    invalidRecords++;
                    logger.error(`Error processing resource ${totalRecords}:`, err.message);
                    logger.error('Invalid resource data:', line);

                    // Log the failed record to the log file
                    logFailedRecord(resource, { raw: line }, err);
                }

                // Log progress every 1000 records
                if (totalRecords % 1000 === 0) {
                    const elapsedTime = (Date.now() - startTime) / 1000; // Elapsed time in seconds
                    const recordsPerSecond = totalRecords / elapsedTime; // Records processed per second
                    const estimatedTotalTime = (totalRecords / recordsPerSecond).toFixed(2); // Estimated total time in seconds
                    const estimatedTimeRemaining = (estimatedTotalTime - elapsedTime).toFixed(2); // Estimated time remaining in seconds

                    logger.info(`Processed ${totalRecords} records (${parsedRecords} parsed, ${invalidRecords} invalid)`);
                    logger.info(`Elapsed time: ${elapsedTime.toFixed(2)} seconds`);
                    logger.info(`Estimated time remaining: ${estimatedTimeRemaining} seconds`);
                }
            }).catch(reject);
        });

        rl.on('close', () => {
            limit(() => {
                const elapsedTime = (Date.now() - startTime) / 1000; // Elapsed time in seconds
                logger.info(`Finished processing NDJSON file. Total records: ${totalRecords}, Parsed records: ${parsedRecords}, Invalid records: ${invalidRecords}`);
                logger.info(`Total elapsed time: ${elapsedTime.toFixed(2)} seconds`);
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