const parser = require('./OracleConfigParser');

export function createClient(uri, databaseOptions) {
  let dbOptions = {};
  databaseOptions = databaseOptions || {};

  if (uri) {
    dbOptions = parser.getDatabaseOptionsFromURI(uri);
  }

  for (const key in databaseOptions) {
    dbOptions[key] = databaseOptions[key];
  }

  const orcl = require('oracledb');

  try {
    orcl.initOracleClient();
  } catch (err) {
    // Oracle client may already be initialized, which is fine
    if (!err.message.includes('DPI-1010') && !err.message.includes('already been initialized')) {
      console.warn('Oracle client initialization warning:', err.message);
    }
  }
  const pool =  orcl.createPool(dbOptions)
    .then(client => {
      return client;
    })
    .catch(error => {
      console.error('Error creating Oracle connection pool:', error);
      throw error;
    });

  if (dbOptions.pgOptions) {
    for (const key in dbOptions.pgOptions) {
      orcl.defaults[key] = dbOptions.pgOptions[key];
    }
  }

  return { pool, orcl };
}
