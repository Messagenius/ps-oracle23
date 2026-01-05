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

  const initOptions = dbOptions.initOptions || {};
  initOptions.noWarnings = process && process.env.TESTING;

  const orcl = require('oracle')(initOptions);
  const client = orcl(dbOptions);

  if (process.env.PARSE_SERVER_LOG_LEVEL === 'debug') {
    const monitor = require('pg-monitor');
    if (monitor.isAttached()) {
      monitor.detach();
    }
    monitor.attach(initOptions);
  }

  if (dbOptions.pgOptions) {
    for (const key in dbOptions.pgOptions) {
      orcl.pg.defaults[key] = dbOptions.pgOptions[key];
    }
  }

  return { client, orcl };
}
