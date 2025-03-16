import logger from './logger.js';

/**
 * Logs a failed record to a log file.
 * @param {string} tableName - The name of the table or resource type.
 * @param {object} record - The failed record (JSON object).
 * @param {Error} error - The error object containing details about the failure.
 */
export function logFailedRecord(tableName, record, error) {
    try {
        const logEntry = {
            timestamp: new Date().toISOString(),
            table: tableName,
            record,
            error: {
                message: error.message,
                stack: error.stack,
            },
        };

        logger.error('Failed record:', logEntry);
    } catch (err) {
        logger.error('Error logging failed record:', err);
    }
}