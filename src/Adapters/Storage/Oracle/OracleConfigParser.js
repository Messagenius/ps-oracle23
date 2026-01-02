const fs = require('fs');
function getDatabaseOptionsFromURI(uri) {
  const databaseOptions = {};

  const parsedURI = new URL(uri);
  const queryParams = parseQueryParams(parsedURI.searchParams.toString());

  databaseOptions.host = parsedURI.hostname || 'localhost';
  databaseOptions.port = parsedURI.port ? parseInt(parsedURI.port) : 1337;
  databaseOptions.database = parsedURI.pathname ? parsedURI.pathname.substring(1) : undefined;

  databaseOptions.user = parsedURI.username;
  databaseOptions.password = parsedURI.password;

  databaseOptions.binary =
    queryParams.binary && queryParams.binary.toLowerCase() === 'true';

  databaseOptions.application_name = queryParams.application_name;
  databaseOptions.fallback_application_name = queryParams.fallback_application_name;

  if (queryParams.poolSize) {
    databaseOptions.poolMax = parseInt(queryParams.poolSize) || 10;
  }
  if (queryParams.max) {
    databaseOptions.poolMax = parseInt(queryParams.max) || 10;
  }

  if (queryParams.min) {
    databaseOptions.poolMin = parseInt(queryParams.min) || 4;
  }

  if (queryParams.increment) {
    databaseOptions.poolIncrement = parseInt(queryParams.increment) || 1;
  }

  if (queryParams.timeout) {
    databaseOptions.poolTimeout = parseInt(queryParams.timeout) || 1000;
  }

  if (queryParams.ping) {
    databaseOptions.poolPingInterval = parseInt(queryParams.ping) || 10;
  }

  return databaseOptions;
}

function parseQueryParams(queryString) {
  queryString = queryString || '';

  return queryString.split('&').reduce((p, c) => {
    const parts = c.split('=');
    p[decodeURIComponent(parts[0])] =
      parts.length > 1 ? decodeURIComponent(parts.slice(1).join('=')) : '';
    return p;
  }, {});
}

module.exports = {
  parseQueryParams: parseQueryParams,
  getDatabaseOptionsFromURI: getDatabaseOptionsFromURI,
};
