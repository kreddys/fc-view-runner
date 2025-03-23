import fs from 'fs';
import path from 'path';
import { parseViewDefinition } from './viewParser.js';
import { processNdjson } from './ndjsonProcessor.js';
import config from './config.js';

/**
 * Scans the bulk export directory and returns a map of resource folders and their corresponding NDJSON files.
 * @param {string} bulkExportDir - Path to the bulk export directory.
 * @returns {Object} - A map where keys are resource folder names and values are arrays of NDJSON file paths.
 */
function scanBulkExportDirectory(bulkExportDir) {
    const resourceFolders = fs.readdirSync(bulkExportDir, { withFileTypes: true })
        .filter(dirent => dirent.isDirectory())
        .map(dirent => dirent.name);

    const resourceToNdjsonMap = {};

    resourceFolders.forEach(folder => {
        const folderPath = path.join(bulkExportDir, folder);
        const ndjsonFiles = fs.readdirSync(folderPath)
            .filter(file => file.endsWith('.ndjson'))
            .map(file => path.join(folderPath, file));

        if (ndjsonFiles.length > 0) {
            resourceToNdjsonMap[folder] = ndjsonFiles;
        }
    });

    return resourceToNdjsonMap;
}

/**
 * Finds ViewDefinitions that match the given resource folder name.
 * @param {string} viewsDir - Path to the Views directory.
 * @param {string} resourceFolderName - Name of the resource folder (e.g., "AllergyIntolerance").
 * @returns {Array} - Array of matching ViewDefinition file paths.
 */
function findMatchingViewDefinitions(viewsDir, resourceFolderName) {
    const viewFiles = fs.readdirSync(viewsDir)
        .filter(file => file.startsWith(`${resourceFolderName}_`) && file.endsWith('.json'));

    return viewFiles.map(file => path.join(viewsDir, file));
}

/**
 * Processes all NDJSON files in a resource folder using the matching ViewDefinitions.
 * @param {string} resourceFolderName - Name of the resource folder (e.g., "AllergyIntolerance").
 * @param {Array} ndjsonFiles - Array of NDJSON file paths in the folder.
 * @param {Array} viewDefinitionFiles - Array of matching ViewDefinition file paths.
 * @param {Object} dbHandler - Database handler instance.
 */
async function processResourceFolder(resourceFolderName, ndjsonFiles, viewDefinitionFiles, dbHandler) {
    for (const viewDefinitionFile of viewDefinitionFiles) {
        console.log(`Processing ViewDefinition: ${viewDefinitionFile}`);

        const viewDefinitionContent = fs.readFileSync(viewDefinitionFile, 'utf8');
        const viewDefinition = JSON.parse(viewDefinitionContent);

        const { columns, whereClauses, resource, constants, select } = parseViewDefinition(viewDefinition);

        let allRows = [];

        // Process all NDJSON files for this resource folder
        for (const ndjsonFile of ndjsonFiles) {
            console.log(`Processing NDJSON file: ${ndjsonFile}`);
            const rows = await processNdjson(ndjsonFile, {
                columns,
                whereClauses,
                resource,
                constants,
                select
            });

            allRows = allRows.concat(rows);
        }

        const tableName = viewDefinition.name.toLowerCase();
        const resourceKey = `${resource.toLowerCase()}_id`; // Determine the resource key dynamically
        await dbHandler.createTable(tableName, columns, resourceKey); // Pass the resource key to createTable

        const upsertResult = await dbHandler.upsertData(tableName, allRows, resourceKey); // Pass the resource key to upsertData

        console.log(`\nSummary for ViewDefinition "${viewDefinition.name}":`);
        console.log(`- Records Parsed: ${allRows.length}`);
        console.log(`- Records Inserted: ${upsertResult.inserted}`);
        console.log(`- Records Updated: ${upsertResult.updated}`);
        console.log(`- Errors: ${upsertResult.errors}`);
        console.log('----------------------------------------');
    }
}

async function main() {
    try {
        const dbHandler = await import('./duckdbHandler.js').then(module => module.default());

        const bulkExportDir = path.resolve(config.bulkExportFolder); // Add bulkExportFolder to config
        const viewsDir = path.resolve(config.viewDefinitionsFolder);

        // Scan the bulk export directory to get resource folders and their NDJSON files
        const resourceToNdjsonMap = scanBulkExportDirectory(bulkExportDir);

        // Process each resource folder
        for (const [resourceFolderName, ndjsonFiles] of Object.entries(resourceToNdjsonMap)) {
            console.log(`Processing resource folder: ${resourceFolderName}`);

            // Find matching ViewDefinitions for this resource folder
            const viewDefinitionFiles = findMatchingViewDefinitions(viewsDir, resourceFolderName);

            if (viewDefinitionFiles.length === 0) {
                console.warn(`No matching ViewDefinitions found for resource folder: ${resourceFolderName}`);
                continue;
            }

            // Process all NDJSON files in this folder using the matching ViewDefinitions
            await processResourceFolder(resourceFolderName, ndjsonFiles, viewDefinitionFiles, dbHandler);
        }
    } catch (err) {
        console.error('Error in main function:', err.message);
        console.error('Error stack:', err.stack);
    }
}

main();