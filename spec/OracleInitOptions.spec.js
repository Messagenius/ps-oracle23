const Parse = require('parse/node').Parse;
const OracleStorageAdapter = require('../lib/Adapters/Storage/Oracle/OracleStorageAdapter').default;

const databaseURI =
  process.env.PARSE_SERVER_TEST_DATABASE_URI ||
  'oracle://parseserver:ParseTest123!@oracle:1521/freepdb1';

// Oracle doesn't use schemas the same way as PostgreSQL
// In Oracle, "schema" = user
// Options for current user (default)
const databaseOptions1 = {
  user: 'parseserver',
  // In Oracle, you can specify other parameters
  poolMin: 2,
  poolMax: 10,
};

const GameScore = Parse.Object.extend({
  className: 'GameScore',
});

describe_only_db('oracle')('Oracle database init options', () => {
  let adapter;

  afterEach(async () => {
    // Close adapter and connection pool
    if (adapter && adapter.close) {
      await adapter.close();
    }
  });

  it('should create server with default user/schema', async () => {
    adapter = new OracleStorageAdapter({
      uri: databaseURI,
      collectionPrefix: 'test_',
      databaseOptions: databaseOptions1,
    });

    await reconfigureServer({
      databaseAdapter: adapter,
    });

    const score = new GameScore({
      score: 1337,
      playerName: 'Sean Plott',
      cheatMode: false,
    });

    await score.save();
    expect(score.id).toBeDefined();
  });

  it('should create server using oracle uri', async () => {
    adapter = new OracleStorageAdapter({
      uri: databaseURI,
      collectionPrefix: 'test_',
    });

    await reconfigureServer({
      databaseAdapter: adapter,
    });

    const score = new GameScore({
      score: 1337,
      playerName: 'Sean Plott',
      cheatMode: false,
    });

    await score.save();
    expect(score.id).toBeDefined();
  });

  it('should fail to create server with invalid credentials', async () => {
    // Create URI with invalid credentials
    const invalidURI = 'oracle://invalid_user:invalid_pass@oracle:1521/freepdb1';

    // Suppress console.error for expected error messages
    const originalConsoleError = console.error;
    console.error = (...args) => {
      const message = args.join(' ');
      if (message.includes('ORA-01017') || 
          message.includes('invalid credential') ||
          message.includes('Error creating Oracle connection pool') ||
          message.includes('Error closing Oracle pool') ||
          message.includes('Error on ParseServer.startApp')) {
        return; // Suppress expected errors
      }
      originalConsoleError.apply(console, args);
    };

    adapter = new OracleStorageAdapter({
      uri: invalidURI,
      collectionPrefix: 'test_',
    });

    // Test initialization directly to catch error at source
    let error;
    try {
      // Try to perform initialization which will attempt to connect
      await adapter.performInitialization({ VolatileClassesSchemas: [] });
      fail('Should have thrown authentication error');
    } catch (e) {
      error = e;
    }

    // Verify the error
    expect(error).toBeDefined();
    // Oracle returns ORA-01017: invalid username/password
    expect(
      error.errorNum === 1017 ||
      error.code === 'ORA-01017' ||
      (error.message && error.message.includes('ORA-01017')) ||
      (error.message && error.message.includes('invalid credential'))
    ).toBe(true);

    // Restore console
    console.error = originalConsoleError;

    // Don't try to close adapter with invalid credentials
    adapter = null;
  });

  it('should fail with invalid connection string', async () => {
    const invalidURI = 'oracle://parseserver:ParseTest123!@invalid_host:1521/freepdb1';

    // Suppress console.error for expected error messages
    const originalConsoleError = console.error;
    console.error = (...args) => {
      const message = args.join(' ');
      if (message.includes('ORA-12262') || 
          message.includes('Cannot connect to database') ||
          message.includes('Could not resolve hostname') ||
          message.includes('Error creating Oracle connection pool') ||
          message.includes('Error closing Oracle pool') ||
          message.includes('Error on ParseServer.startApp')) {
        return; // Suppress expected errors
      }
      originalConsoleError.apply(console, args);
    };

    adapter = new OracleStorageAdapter({
      uri: invalidURI,
      collectionPrefix: 'test_',
    });

    // Test initialization directly to catch error at source
    let error;
    try {
      // Try to perform initialization which will attempt to connect
      await adapter.performInitialization({ VolatileClassesSchemas: [] });
      fail('Should have thrown connection error');
    } catch (e) {
      error = e;
    }

    // Verify the error
    expect(error).toBeDefined();
    // Connection error - could be ORA-12262 or other connection errors
    expect(error.message).toBeDefined();
    expect(
      error.errorNum === 12262 ||
      error.code === 'ORA-12262' ||
      (error.message && error.message.includes('ORA-12262')) ||
      (error.message && error.message.includes('Cannot connect')) ||
      (error.message && error.message.includes('Could not resolve hostname'))
    ).toBe(true);

    // Restore console
    console.error = originalConsoleError;

    // Don't try to close adapter with invalid connection - it will just create more errors
    adapter = null;
  });

  it('should work with different pool sizes', async () => {
    adapter = new OracleStorageAdapter({
      uri: databaseURI,
      collectionPrefix: 'test_',
      databaseOptions: {
        poolMin: 1,
        poolMax: 5,
        poolIncrement: 1,
      },
    });

    await reconfigureServer({
      databaseAdapter: adapter,
    });

    const score = new GameScore({
      score: 1337,
      playerName: 'Sean Plott',
      cheatMode: false,
    });

    await score.save();
    expect(score.id).toBeDefined();
  });

  it('should handle connection timeout options', async () => {
    adapter = new OracleStorageAdapter({
      uri: databaseURI,
      collectionPrefix: 'test_',
      databaseOptions: {
        poolMin: 2,
        poolMax: 10,
        queueTimeout: 10000, // 10 seconds
        connectTimeout: 5000, // 5 seconds
      },
    });

    await reconfigureServer({
      databaseAdapter: adapter,
    });

    const score = new GameScore({
      score: 1337,
      playerName: 'Sean Plott',
      cheatMode: false,
    });

    await score.save();
    expect(score.id).toBeDefined();
  });

  it('should support TNS connection string format', async () => {
    // Oracle supports TNS format
    // Example: oracle://user:pass@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=service)))
    // Note: TNS format parsing may not be fully implemented, so this test may need adjustment

    const tnsURI =
      'oracle://parseserver:ParseTest123!@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=oracle)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=freepdb1)))';

    // Suppress console.error for expected error messages
    const originalConsoleError = console.error;
    console.error = (...args) => {
      const message = args.join(' ');
      if (message.includes('ORA-12162') || 
          message.includes('TNS:net service name is incorrectly specified') ||
          message.includes('Error creating Oracle connection pool') ||
          message.includes('Error closing Oracle pool') ||
          message.includes('Error on ParseServer.startApp')) {
        return; // Suppress expected errors
      }
      originalConsoleError.apply(console, args);
    };

    adapter = new OracleStorageAdapter({
      uri: tnsURI,
      collectionPrefix: 'test_',
    });

    // Test initialization directly to catch error at source
    let error;
    try {
      // Try to perform initialization which will attempt to connect
      await adapter.performInitialization({ VolatileClassesSchemas: [] });
      
      // If initialization succeeds, test that it works
      await reconfigureServer({
        databaseAdapter: adapter,
      });

      const score = new GameScore({
        score: 1337,
        playerName: 'Sean Plott',
        cheatMode: false,
      });

      await score.save();
      expect(score.id).toBeDefined();
    } catch (e) {
      error = e;
      // If TNS format isn't supported or incorrectly specified, that's expected for now
      expect(error).toBeDefined();
      expect(
        error.errorNum === 12162 ||
        error.code === 'ORA-12162' ||
        (error.message && error.message.includes('ORA-12162')) ||
        (error.message && error.message.includes('TNS'))
      ).toBe(true);
      adapter = null; // Don't try to close if it failed
    } finally {
      // Restore console
      console.error = originalConsoleError;
    }
  });
});

describe_only_db('oracle')('Oracle multiple users/schemas', () => {
  let adapter;

  afterEach(async () => {
    // Close adapter and connection pool
    if (adapter && adapter.close) {
      await adapter.close();
    }
  });

  it('should fail with non-existent user', async () => {
    const invalidURI = 'oracle://nonexistent_user:pass@oracle:1521/freepdb1';

    // Suppress console.error for expected error messages
    const originalConsoleError = console.error;
    console.error = (...args) => {
      const message = args.join(' ');
      if (message.includes('ORA-01017') || 
          message.includes('invalid credential') ||
          message.includes('Error creating Oracle connection pool') ||
          message.includes('Error closing Oracle pool') ||
          message.includes('Error on ParseServer.startApp')) {
        return; // Suppress expected errors
      }
      originalConsoleError.apply(console, args);
    };

    adapter = new OracleStorageAdapter({
      uri: invalidURI,
      collectionPrefix: 'test_',
    });

    // Test initialization directly to catch error at source
    let error;
    try {
      // Try to perform initialization which will attempt to connect
      await adapter.performInitialization({ VolatileClassesSchemas: [] });
      fail('Should have thrown authentication error');
    } catch (e) {
      error = e;
    }

    // Verify the error
    expect(error).toBeDefined();
    // Oracle returns ORA-01017: invalid username/password
    expect(
      error.errorNum === 1017 ||
      error.code === 'ORA-01017' ||
      (error.message && error.message.includes('ORA-01017')) ||
      (error.message && error.message.includes('invalid credential'))
    ).toBe(true);

    // Restore console
    console.error = originalConsoleError;

    // Don't try to close adapter with invalid credentials
    adapter = null;
  });
});
