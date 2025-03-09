const fs = require('fs');
const path = require('path');
const { parseViewDefinition } = require('./viewParser');
const { processNdjson } = require('./ndjsonProcessor');
const { createTable, upsertData } = require('./duckdbHandler');

/**
 * Scan a folder for JSON files containing ViewDefinition resources.
 * @param {string} folderPath - Path to the folder.
 * @returns {Array} - Array of ViewDefinition objects.
 */
function scanViewDefinitions(folderPath) {
    const viewDefinitions = [];
    const files = fs.readdirSync(folderPath);

    files.forEach((file) => {
        if (path.extname(file) === '.json') {
            const filePath = path.join(folderPath, file);
            const content = fs.readFileSync(filePath, 'utf8');
            try {
                const viewDefinition = JSON.parse(content);
                if (viewDefinition.resourceType === 'http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition') {
                    viewDefinitions.push(viewDefinition);
                }
            } catch (err) {
                console.error(`Error parsing file ${file}:`, err);
            }
        }
    });

    return viewDefinitions;
}

// In index.js, update the main function:
async function main() {
    try {
        // Get the database handler
        const dbHandler = await require('./duckdbHandler')();

        // Example ViewDefinitions folder and NDJSON file path
        const viewDefinitionsFolder = './definitions';
        const ndjsonFilePath = './data/ndjson/sample_resources.ndjson';

        // Scan the folder for ViewDefinition files
        const viewDefinitions = scanViewDefinitions(viewDefinitionsFolder);
        console.log(`Found ${viewDefinitions.length} ViewDefinition(s) in folder.`);

        // Process each ViewDefinition
        for (const viewDefinition of viewDefinitions) {
            console.log(`Processing ViewDefinition: ${viewDefinition.name}`);

            // Parse ViewDefinition
            const { columns, whereClauses } = parseViewDefinition(viewDefinition);

            // Process NDJSON file
            const rows = await processNdjson(ndjsonFilePath, columns, whereClauses);

            // Create table in DuckDB (if it doesn't exist)
            const tableName = viewDefinition.name.toLowerCase();
            await dbHandler.createTable(tableName, columns);

            // Upsert data into DuckDB
            const primaryKey = `${tableName}_id`; // e.g., "observation_id" or "patient_id"
            await dbHandler.upsertData(tableName, rows, primaryKey);

            console.log(`Data for ViewDefinition "${viewDefinition.name}" successfully upserted into DuckDB!`);
        }
    } catch (err) {
        console.error('Error in main function:', err);
    }
}

main();