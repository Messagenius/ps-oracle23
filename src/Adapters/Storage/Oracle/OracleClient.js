import orcl from 'oracledb';

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
