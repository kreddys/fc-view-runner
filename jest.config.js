export default {
  testEnvironment: 'node', // Use Node.js environment
  testMatch: ['**/__tests__/**/*.test.js'], // Match test files
  collectCoverage: true, // Enable coverage collection
  coverageDirectory: 'coverage', // Output directory for coverage reports
  coverageReporters: ['text', 'lcov'], // Coverage report formats
  setupFilesAfterEnv: ['./__tests__/setup.js'], // Setup files
  transform: {}, // Disable Babel transformation
  extensionsToTreatAsEsm: ['.js'], // Treat .js files as ES Modules
};