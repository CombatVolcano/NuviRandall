# Nuvi code project for Randall
Processes ZIP files as specified;

  - Resolves the given remote URL until HTTPOK 
  - Downloads the ZIP documents
  - Extracts the XML files
  - Adds XML data to a Redis list (using a set first to avoid duplication)

Running Tests
  - Tests are written with rspec
  - Comment bottom 2 lines in NuviProject.rb before running tests
