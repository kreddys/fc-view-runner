const config = require('./config');

function logDebug(message) {
    if (config.debug) {
        console.log(message);
    }
}

/**
 * Parse a FHIR ViewDefinition resource and extract column paths, names, and types.
 * @param {Object} viewDefinition - The ViewDefinition resource.
 * @returns {Object} - Object containing columns and where clauses.
 */
function parseViewDefinition(viewDefinition) {
    logDebug(`Parsing ViewDefinition: ${JSON.stringify(viewDefinition, null, 2)}`);

    if (!viewDefinition.select || !Array.isArray(viewDefinition.select)) {
        throw new Error('Invalid ViewDefinition: Missing select definitions.');
    }

    const columns = [];
    viewDefinition.select.forEach((select) => {
        if (select.column && Array.isArray(select.column)) {
            select.column.forEach((col) => {
                columns.push({
                    path: col.path,
                    name: col.name,
                    type: col.type || 'string', // Default to 'string' if type is not provided
                    description: col.description || '',
                    collection: col.collection || false,
                });
            });
        }
    });

    const whereClauses = [];
    if (viewDefinition.where && Array.isArray(viewDefinition.where)) {
        viewDefinition.where.forEach((where) => {
            whereClauses.push({
                path: where.path,
                description: where.description || '',
            });
        });
    }

    logDebug(`Extracted columns: ${JSON.stringify(columns, null, 2)}`);
    logDebug(`Extracted where clauses: ${JSON.stringify(whereClauses, null, 2)}`);

    return { columns, whereClauses };
}

module.exports = {
    parseViewDefinition,
};