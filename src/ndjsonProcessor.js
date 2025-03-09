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
    },
};

function processNdjson(filePath, columns, whereClauses) {
    const rows = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(ndjson.parse())
            .on('data', (resource) => {
                logDebug('Processing resource:', resource);

                let includeResource = true;
                whereClauses.forEach((where) => {
                    const result = fhirpath.evaluate(
                        resource,
                        where.path,
                        null,
                        fhirpath_r4_model,
                        { userInvocationTable: customFunctions }
                    );
                    if (!result || result.length === 0 || !result[0]) {
                        includeResource = false;
                    }
                });

                if (includeResource) {
                    const row = {};
                    columns.forEach((col) => {
                        try {
                            const result = fhirpath.evaluate(
                                resource,
                                col.path,
                                null,
                                fhirpath_r4_model,
                                { userInvocationTable: customFunctions }
                            );
                            logDebug(`Evaluated path "${col.path}":`, result);
                            row[col.name] = col.collection ? result : result.length > 0 ? result[0] : null;
                        } catch (err) {
                            console.error(`Error evaluating path "${col.path}" on resource:`, resource);
                            console.error(err);
                            row[col.name] = null;
                        }
                    });
                    rows.push(row);
                }
            })
            .on('end', () => {
                logDebug('Finished processing NDJSON file.');
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