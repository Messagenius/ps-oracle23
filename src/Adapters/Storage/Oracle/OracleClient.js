const parser = require('./OracleConfigParser');

export async function createClient(uri, databaseOptions) {
  let dbOptions = {};
  databaseOptions = databaseOptions || {};

  if (uri) {
    dbOptions = parser.getDatabaseOptionsFromURI(uri);
  }

  for (const key in databaseOptions) {
    dbOptions[key] = databaseOptions[key];
  }

  const initOptions = dbOptions.initOptions || {};
  initOptions.noWarnings = process && process.env.TESTING;

  const orcl = require('oracledb');
  await orcl.createPool(dbOptions);

  const client = orcl.getConnection();

  return { client, orcl };
}
