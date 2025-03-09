# FHIR Data Processor with DuckDB

This project processes FHIR (Fast Healthcare Interoperability Resources) data from NDJSON files and stores it in a DuckDB database. It supports both synchronous and asynchronous processing modes, with configurable concurrency and batch processing.

## Features
- **NDJSON File Processing**: Parses FHIR data from NDJSON files.
- **DuckDB Integration**: Stores processed data in a DuckDB database.
- **Async and Sync Modes**: Supports both synchronous and asynchronous processing.
- **Configurable Concurrency**: Control the number of concurrent operations.
- **Batch Processing**: Processes data in configurable batches for better performance.

## Environment Variables
The following environment variables are used to configure the application:

| Variable                | Description                                            | Default Value                                  |
|-------------------------|--------------------------------------------------------|------------------------------------------------|
| DEBUG                   | Enable debug logging (true or false).                 | false                                          |
| VIEW_DEFINITIONS_FOLDER | Path to the folder containing ViewDefinition JSON files. | ./definitions                                  |
| NDJSON_FILE_PATH        | Path to the NDJSON file containing FHIR data.          | ./data/ndjson/sample-data.ndjson               |
| DUCKDB_FOLDER           | Folder where the DuckDB database will be stored.       | ./data/duckdb                                  |
| DUCKDB_FILE_NAME        | Name of the DuckDB database file.                      | fhir_data.db                                   |
| ASYNC_PROCESSING        | Enable asynchronous processing (true or false).        | true                                           |
| CONNECTION_POOL_SIZE    | Number of connections in the DuckDB connection pool.  | 10                                             |
| CONCURRENCY_LIMIT       | Maximum number of concurrent operations in async mode. | 10                                             |
| BATCH_SIZE              | Number of rows to process in each batch.               | 1000                                           |

## Setup
### 1. Prerequisites
- Node.js (v16 or higher)
- npm (Node Package Manager)

### 2. Install Dependencies
Run the following command to install the required dependencies:

```bash
npm install
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory of the project and add the required environment variables. For example:

```env
DEBUG=false
VIEW_DEFINITIONS_FOLDER=./definitions
NDJSON_FILE_PATH=./data/ndjson/sample-data.ndjson
DUCKDB_FOLDER=./data/duckdb
DUCKDB_FILE_NAME=fhir_data.db

ASYNC_PROCESSING=true
CONNECTION_POOL_SIZE=10
CONCURRENCY_LIMIT=10
BATCH_SIZE=1000
```

### 4. Prepare Data
- Place your ViewDefinition JSON files in the `VIEW_DEFINITIONS_FOLDER`.
- Place your NDJSON file at the `NDJSON_FILE_PATH`.

## Usage
### 1. Run the Application
To start the application, run the following command:

```bash
npm start
```

### 2. Logs
- The application logs progress and errors to the console.
- If `DEBUG=true`, detailed debug logs will be printed.

### 3. Output
- Processed data is stored in the DuckDB database at the specified location (`DUCKDB_FOLDER/DUCKDB_FILE_NAME`).
- Summary statistics (e.g., records parsed, inserted, updated, errors) are logged for each ViewDefinition.

## Example
### Input
- **ViewDefinition**: A JSON file defining the structure of the data to be processed.
- **NDJSON File**: A file containing FHIR resources in NDJSON format.

### Output
- **DuckDB Database**: A database file containing the processed data.

### Sample Logs
```log
Found 2 ViewDefinition(s) in folder.
Processing ViewDefinition: Observation
Finished processing NDJSON file. Total records: 9878, Parsed records: 9878
Table "observation" already exists. Skipping creation.
Processing batch 1 of 10
Processed batch 1: Upserted 1000 of 9878 rows (Inserted: 1000, Updated: 0, Errors: 0)
...
Finished processing NDJSON file. Total records: 9878, Parsed records: 9878
```

## Troubleshooting
### 1. Segmentation Fault
If you encounter a segmentation fault:
- Reduce the `CONCURRENCY_LIMIT` and `CONNECTION_POOL_SIZE` values.
- Ensure the NDJSON file is properly formatted and does not contain invalid data.

### 2. Missing Data
If some rows are not processed:
- Check the logs for errors related to invalid data.
- Ensure the ViewDefinition matches the structure of the NDJSON file.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## Contact
For questions or feedback, please contact [Your Name] at [your.email@example.com].
