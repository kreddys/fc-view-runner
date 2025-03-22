# Testing Commands for view-runner

This document provides a list of useful commands for running specific tests or test suites in the view-runner project.

## 1. Run All Tests
To run all test suites and test cases:

```bash
npm test
```

## 2. Run a Specific Test Suite
To run a specific test suite (e.g., NdjsonProcessor):

```bash
npm test -- -t "NdjsonProcessor"
```

## 3. Run a Specific Test Case
To run a specific test case within a suite (e.g., should extract category using getCurrentContext in forEach):

```bash
npm test -- -t "should extract category using getCurrentContext in forEach"
```

## 4. Run Tests in Watch Mode
To run tests in watch mode (automatically re-runs tests when files change):

```bash
npm run test:watch
```

## 5. Run Tests with Increased Memory Limit
If you encounter memory issues during testing, you can increase the memory limit:

```bash
node --max-old-space-size=8192 node_modules/.bin/jest
```

## 6. Run a Single Test File
To run tests from a specific file (e.g., ndjsonProcessor.test.js):

```bash
npm test -- tests/ndjsonProcessor.test.js
```

## 7. Run Tests Matching a Pattern
To run tests that match a specific pattern (e.g., all tests containing the word "extension"):

```bash
npm test -- -t "extension"
```

## 8. Isolate Tests Temporarily
During development, you can isolate specific test suites or cases using `describe.only` or `it.only`:

### Isolate a Test Suite

```javascript
describe.only('NdjsonProcessor', () => {
  // Only this suite will run
});
```

### Isolate a Test Case

```javascript
it.only('should extract current context using getCurrentContext in forEach', async () => {
  // Only this test will run
});
```

Then, run:

```bash
npm test
```

## 9. Debugging Tests
To debug tests, use the `--inspect-brk` flag with Node.js:

```bash
node --inspect-brk node_modules/.bin/jest --runInBand
```

Open Chrome and navigate to `chrome://inspect`. Click on "Open dedicated DevTools for Node" to start debugging.

## 10. Generate Code Coverage Report
To generate a code coverage report:

```bash
npm test -- --coverage
```

The report will be saved in the `coverage` directory. Open `coverage/lcov-report/index.html` in your browser to view the report.

## 11. List All Available Test Suites
To list all available test suites without running them:

```bash
npm test -- --listTests
```

## 12. Run Tests in Silent Mode
To run tests without showing verbose output:

```bash
npm test -- --silent
```

## 13. Update Snapshots
If you’re using Jest snapshots and need to update them:

```bash
npm test -- -u
```

## 14. Run Tests in a Specific Environment
To run tests in a specific environment (e.g., test):

```bash
NODE_ENV=test npm test
```

## 15. Run Tests with Custom Configuration
To run tests with a custom Jest configuration file (e.g., `jest.custom.config.js`):

```bash
npm test -- --config jest.custom.config.js
```

## 16. Run Tests with Verbose Output
To run tests with detailed output for debugging:

```bash
npm test -- --verbose
```

## 17. Run Tests and Exit on First Failure
To stop test execution after the first failure:

```bash
npm test -- --bail
```

## 18. Run Tests with a Specific Reporter
To use a specific test reporter (e.g., summary):

```bash
npm test -- --reporters summary
```

## 19. Run Tests with a Timeout
To set a custom timeout for tests (e.g., 10 seconds):

```bash
npm test -- --testTimeout 10000
```

## 20. Run Tests and Show Only Failures
To show only failing tests in the output:

```bash
npm test -- --onlyFailures
```

## Tips
- Use `describe.only` and `it.only` during development to focus on specific tests.
- Use `--watch` mode to automatically re-run tests when files change.
- Use `--coverage` to generate a code coverage report and identify untested code.

