# FHIR NDJSON to DuckDB View Runner

This project processes FHIR (Fast Healthcare Interoperability Resources) data from NDJSON files and stores it in a DuckDB database. It uses SQL on FHIR ViewDefinitions to define how FHIR resources are parsed and transformed into a flat structure suitable for storage in a relational database.

## Key Features
- **NDJSON File Processing**: Parses FHIR data from NDJSON files.
- **DuckDB Integration**: Stores processed data in a DuckDB database.
- **SQL on FHIR ViewDefinitions**: Defines how FHIR resources are parsed and transformed using FHIRPath.
- **Real-Time Logging**: Provides progress updates during processing.
- **Error Handling**: Skips invalid rows and logs them for debugging.
- **Configurable Concurrency**: Supports both synchronous and asynchronous processing.

## How It Works
1. **ViewDefinitions**: JSON files define the structure of the data to be extracted from FHIR resources using FHIRPath expressions.
2. **NDJSON Processing**: The application reads the NDJSON file line by line, parses each FHIR resource, and applies the ViewDefinition to extract data.
3. **DuckDB Storage**: The extracted data is stored in a DuckDB database, with support for batch processing and upserts.

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
Create a `.env` file in the root directory and add the following:

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
### Run the Application
To start the application, run the following command:

```bash
npm start
```

### Logs
- Progress and errors are logged to the console in real-time.
- If `DEBUG=true`, detailed debug logs will be printed.

## Example
### Input
- **ViewDefinition**: A JSON file defining the structure of the data to be processed.
- **NDJSON File**: A file containing FHIR resources in NDJSON format.

### Output
- **DuckDB Database**: A database file containing the processed data.

## Troubleshooting
- **Segmentation Fault**: Reduce `CONCURRENCY_LIMIT` and `CONNECTION_POOL_SIZE`.
- **Missing Data**: Check logs for errors and ensure the ViewDefinition matches the NDJSON file structure.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact
For questions or feedback, please contact [Kishore Reddy] at [kishore5214@outlook.com].

## SQL on FHIR ViewDefinitions
This project follows the SQL on FHIR V2 ViewDefinition specification, which provides a standard format for defining tabular views of FHIR data. These views make FHIR data easier to consume and analyze using generic tools like SQL. For more details, refer to the SQL on FHIR specification.
