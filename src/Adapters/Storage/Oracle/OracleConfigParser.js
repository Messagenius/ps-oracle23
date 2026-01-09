const fs = require('fs');
function getDatabaseOptionsFromURI(uri) {
  const databaseOptions = {};

  const parsedURI = new URL(uri);
  const queryParams = parseQueryParams(parsedURI.searchParams.toString());

  databaseOptions.host = parsedURI.hostname || 'localhost';
  databaseOptions.port = parsedURI.port ? parseInt(parsedURI.port) : 1521;
  databaseOptions.connectString = parsedURI.pathname
    ? databaseOptions.host + parsedURI.pathname
    : undefined;

  databaseOptions.user = parsedURI.username;
  databaseOptions.password = parsedURI.password;

  databaseOptions.fallback_application_name = queryParams.fallback_application_name;

  if (queryParams.poolSize) {
    databaseOptions.poolMax = parseInt(queryParams.poolSize) || 10;
  } else {
    databaseOptions.poolMax = 10;
  }
  if (queryParams.max) {
    databaseOptions.poolMax = parseInt(queryParams.max) || 10;
  }

  if (queryParams.min) {
    databaseOptions.poolMin = parseInt(queryParams.min) || 4;
  } else {
    databaseOptions.poolMin = 4;
  }

  if (queryParams.increment) {
    databaseOptions.poolIncrement = parseInt(queryParams.increment) || 1;
  } else {
    databaseOptions.poolIncrement = 1;
  }

  if (queryParams.timeout) {
    databaseOptions.poolTimeout = parseInt(queryParams.timeout) || 60;
  } else {
    databaseOptions.poolTimeout = 60;
  }

  databaseOptions.queueTimeout = databaseOptions.poolTimeout * 1000;

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
