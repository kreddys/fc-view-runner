import fs from 'fs';
import path from 'path';
import { parseViewDefinition } from './viewParser.js';
import { processNdjson } from './ndjsonProcessor.js';
import config from './config.js';

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

async function main() {
    try {
        const dbHandler = await import('./duckdbHandler.js').then(module => module.default());

        const viewDefinitions = scanViewDefinitions(config.viewDefinitionsFolder);
        console.log(`Found ${viewDefinitions.length} ViewDefinition(s) in folder.`);

        for (const viewDefinition of viewDefinitions) {
            console.log(`Processing ViewDefinition: ${viewDefinition.name}`);

            const startTime = Date.now(); // Start timer

            const { columns, whereClauses, resource, constants, select } = parseViewDefinition(viewDefinition);

            // Get base columns (columns not in forEach blocks)
            const baseColumns = select.reduce((acc, selectDef) => {
                // If this select definition doesn't have forEach, add its columns
                if (!selectDef.forEach && selectDef.column) {
                    acc.push(...selectDef.column);
                }
                return acc;
            }, []);

            const rows = await processNdjson(config.ndjsonFilePath, {
                columns: baseColumns,  // Pass only base columns
                whereClauses,
                resource,
                constants,
                select
            });

            const tableName = viewDefinition.name.toLowerCase();
            await dbHandler.createTable(tableName, columns);

            // Determine the resourceKey dynamically
            const resourceKey = `${resource.toLowerCase()}_id`; // e.g., "patient_id" for "Patient" resource

            const upsertResult = await dbHandler.upsertData(tableName, rows, resourceKey);

            const endTime = Date.now(); // End timer
            const timeTaken = (endTime - startTime) / 1000; // Convert to seconds

            // Log summary
            console.log(`\nSummary for ViewDefinition "${viewDefinition.name}":`);
            console.log(`- Records Parsed: ${rows.length}`);
            console.log(`- Records Inserted: ${upsertResult.inserted}`);
            console.log(`- Records Updated: ${upsertResult.updated}`);
            console.log(`- Errors: ${upsertResult.errors}`);
            console.log(`- Time Taken: ${timeTaken.toFixed(2)} seconds`);
            console.log('----------------------------------------');

            console.log(`Data for ViewDefinition "${viewDefinition.name}" successfully upserted into DuckDB!`);
        }
    } catch (err) {
        console.error('Error in main function:', err.message);
        console.error('Error stack:', err.stack);
    }
}

main();