import config from './config.js';

function logDebug(message) {
    if (config.debug) {
        console.log(`[DEBUG] ${new Date().toISOString()} - ${message}`);
    }
}

function validateViewDefinition(viewDefinition) {
    const required = ['name', 'status', 'resource', 'select'];
    required.forEach(field => {
        if (!viewDefinition[field]) {
            throw new Error(`Invalid ViewDefinition: Missing required field '${field}'`);
        }
    });

    if (!Array.isArray(viewDefinition.select)) {
        throw new Error('Invalid ViewDefinition: select must be an array');
    }
}

function extractMetadata(viewDefinition) {
    return {
        url: viewDefinition.url,
        identifier: viewDefinition.identifier,
        name: viewDefinition.name,
        title: viewDefinition.title,
        status: viewDefinition.status,
        experimental: viewDefinition.experimental,
        publisher: viewDefinition.publisher,
        description: viewDefinition.description,
        resource: viewDefinition.resource,
        fhirVersion: viewDefinition.fhirVersion
    };
}

function extractConstants(viewDefinition) {
    if (!viewDefinition.constant) return [];

    return viewDefinition.constant.map(constant => {
        const valueKey = Object.keys(constant).find(k => k.startsWith('value'));
        return {
            name: constant.name,
            value: constant[valueKey],
            type: valueKey.replace('value', '').toLowerCase()
        };
    });
}

function extractSelects(selects, parentPath = '') {
    const columns = [];
    const nestedSelects = [];

    selects.forEach((select, index) => {
        const currentPath = parentPath ? `${parentPath}.${index}` : String(index);

        if (select.column) {
            select.column.forEach(col => {
                validateColumnName(col.name);
                columns.push({
                    path: col.path,
                    name: col.name,
                    type: col.type || 'string',
                    description: col.description || '',
                    collection: col.collection || false,
                    tags: col.tag || [],
                    selectPath: currentPath
                });
            });
        }

        if (select.select || select.forEach || select.forEachOrNull) {
            const nestedSelect = {
                path: currentPath,
                forEach: select.forEach,
                forEachOrNull: select.forEachOrNull,
                column: select.column || [],
                select: select.select ? extractSelects(select.select, currentPath).columns : []
            };
            nestedSelects.push(nestedSelect);
        }

        if (select.unionAll) {
            const unionResults = extractSelects(select.unionAll, `${currentPath}.union`);
            columns.push(...unionResults.columns);
            nestedSelects.push(...unionResults.nestedSelects);
        }
    });

    return { columns, nestedSelects };
}

function validateColumnName(name) {
    const validNamePattern = /^[A-Za-z][A-Za-z0-9_]*$/;
    if (!validNamePattern.test(name)) {
        throw new Error(`Invalid column name: ${name}. Must start with a letter and contain only letters, numbers, and underscores.`);
    }
}

function extractWhereClauses(viewDefinition) {
    return (viewDefinition.where || []).map(where => ({
        path: where.path,
        description: where.description || ''
    }));
}

function parseViewDefinition(viewDefinition) {
    logDebug(`Parsing ViewDefinition: ${JSON.stringify(viewDefinition, null, 2)}`);

    validateViewDefinition(viewDefinition);

    const { columns, nestedSelects } = extractSelects(viewDefinition.select);

    return {
        metadata: extractMetadata(viewDefinition),
        constants: extractConstants(viewDefinition),
        columns,
        nestedSelects,
        whereClauses: extractWhereClauses(viewDefinition),
        resource: viewDefinition.resource,
        select: viewDefinition.select
    };
}

export { parseViewDefinition };