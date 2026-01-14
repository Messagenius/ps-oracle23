// @flow
import { createClient } from './OracleClient';
// @flow-disable-next
import Parse from 'parse/node';
// @flow-disable-next
import _ from 'lodash';
// @flow-disable-next
import { v4 as uuidv4 } from 'uuid';
import sql from './sql';
import type { QueryType, SchemaType } from '../StorageAdapter';
import { StorageAdapter } from '../StorageAdapter';

import oracledb from 'oracledb';
import orcl from 'oracledb';

const Utils = require('../../../Utils');

const OracleDuplicateRelationError = '42P07';
const logger = require('../../../logger');

const debug = function (...args: any) {
  args = ['ORACLE: ' + arguments[0]].concat(args.slice(1, args.length));
  const log = logger.getLogger();
  log.debug.apply(log, args);
};

class DDLQueue {
  _queues: Map<string, Promise<any>> = new Map();

  async enqueue<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const currentQueue = this._queues.get(key) || Promise.resolve();

    const newQueue = currentQueue
      .then(() => fn())
      .catch(error => {
        console.error(`Error in DDL queue for ${key}:`, error);
        throw error;
      });

    this._queues.set(key, newQueue);

    newQueue.finally(() => {
      if (this._queues.get(key) === newQueue) {
        this._queues.delete(key);
      }
    });

    return newQueue;
  }

  getActiveQueuesCount(): number {
    return this._queues.size;
  }
}

const ddlQueue = new DDLQueue();

// Утилита для повторных попыток
async function retryOnLock<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> {
  let lastError: any;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // ORA-00054: resource busy
      // ORA-00060: deadlock detected
      const shouldRetry = (error.errorNum === 54 || error.errorNum === 60) && attempt < maxRetries;

      if (shouldRetry) {
        const delay = baseDelay * Math.pow(2, attempt);
        console.log(
          `Lock detected (ORA-${String(error.errorNum).padStart(
            5,
            '0'
          )}), retrying in ${delay}ms...`
        );
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      throw error;
    }
  }

  throw lastError;
}

export function getDDLQueueStats() {
  return {
    activeQueues: ddlQueue.getActiveQueuesCount(),
  };
}

const parseTypeToOracleType = type => {
  switch (type.type) {
    case 'String':
      return 'VARCHAR2(4000)';
    case 'Date':
      return 'TIMESTAMP WITH TIME ZONE';
    case 'Object':
      return 'JSON';
    case 'File':
      return 'CLOB';
    case 'Boolean':
      return 'BOOLEAN';
    case 'Pointer':
      return 'VARCHAR2(4000)';
    case 'Number':
      return 'NUMBER';
    case 'GeoPoint':
      return 'SDO_GEOMETRY';
    case 'Bytes':
      return 'JSON';
    case 'Polygon':
      return 'SDO_GEOMETRY';
    case 'Array':
      return 'JSON';
    default:
      throw `no type for ${JSON.stringify(type)} yet`;
  }
};

const ParseToOracleComparator = {
  $gt: '>',
  $lt: '<',
  $gte: '>=',
  $lte: '<=',
};

const mongoAggregateToOracle = {
  $dayOfMonth: 'DD',
  $dayOfWeek: 'D',
  $dayOfYear: 'DDD',
  $isoDayOfWeek: 'ID',
  $isoWeekYear: 'IYYY',
  $hour: 'HH24',
  $minute: 'MI',
  $second: 'SS',
  $millisecond: 'FF3',
  $month: 'MM',
  $week: 'IW',
  $year: 'YYYY',
};

const toOracleValue = value => {
  if (typeof value === 'object') {
    if (value.__type === 'Date') {
      return value.iso;
    }
    if (value.__type === 'File') {
      return value.name;
    }
  }
  return value;
};

const toOracleValueCastType = value => {
  const OracleValue = toOracleValue(value);
  let castType;
  switch (typeof OracleValue) {
    case 'number':
      castType = 'DOUBLE PRECISION';
      break;
    case 'boolean':
      castType = 'BOOLEAN';
      break;
    default:
      castType = undefined;
  }
  return castType;
};

const transformValue = value => {
  if (typeof value === 'object' && value.__type === 'Pointer') {
    return value.objectId;
  }
  return value;
};

// Duplicate from then mongo adapter...
const emptyCLPS = Object.freeze({
  find: {},
  get: {},
  count: {},
  create: {},
  update: {},
  delete: {},
  addField: {},
  protectedFields: {},
});

const defaultCLPS = Object.freeze({
  find: { '*': true },
  get: { '*': true },
  count: { '*': true },
  create: { '*': true },
  update: { '*': true },
  delete: { '*': true },
  addField: { '*': true },
  protectedFields: { '*': [] },
});

const toParseSchema = schema => {
  if (schema.className === '_User') {
    delete schema.fields._hashed_password;
  }
  if (schema.fields) {
    delete schema.fields._wperm;
    delete schema.fields._rperm;
  }
  let clps = defaultCLPS;
  if (schema.classLevelPermissions) {
    clps = { ...emptyCLPS, ...schema.classLevelPermissions };
  }
  let indexes = {};
  if (schema.indexes) {
    indexes = { ...schema.indexes };
  }
  return {
    className: schema.className,
    fields: schema.fields,
    classLevelPermissions: clps,
    indexes,
  };
};

const toOracleSchema = schema => {
  if (!schema) {
    return schema;
  }
  schema.fields = schema.fields || {};
  schema.fields._wperm = { type: 'Array', contents: { type: 'String' } };
  schema.fields._rperm = { type: 'Array', contents: { type: 'String' } };
  if (schema.className === '_User') {
    schema.fields._hashed_password = { type: 'String' };
    schema.fields._password_history = { type: 'Array' };
  }
  return schema;
};

const isArrayIndex = arrayIndex => Array.from(arrayIndex).every(c => c >= '0' && c <= '9');

const handleDotFields = object => {
  Object.keys(object).forEach(fieldName => {
    if (fieldName.indexOf('.') > -1) {
      const components = fieldName.split('.');
      const first = components.shift();
      object[first] = object[first] || {};
      let currentObj = object[first];
      let next;
      let value = object[fieldName];
      if (value && value.__op === 'Delete') {
        value = undefined;
      }
      /* eslint-disable no-cond-assign */
      while ((next = components.shift())) {
        /* eslint-enable no-cond-assign */
        currentObj[next] = currentObj[next] || {};
        if (components.length === 0) {
          currentObj[next] = value;
        }
        currentObj = currentObj[next];
      }
      delete object[fieldName];
    }
  });
  return object;
};

const transformDotFieldToComponents = fieldName => {
  return fieldName.split('.').map((cmpt, index) => {
    if (index === 0) {
      return `"${cmpt}"`;
    }
    if (isArrayIndex(cmpt)) {
      return Number(cmpt);
    } else {
      // Use double quotes for JSON path keys (not single quotes)
      return `"${cmpt}"`;
    }
  });
};

const transformDotField = fieldName => {
  if (fieldName.indexOf('.') === -1) {
    return `"${fieldName}"`;
  }
  const components = transformDotFieldToComponents(fieldName);
  if (components.length === 1) {
    return components[0];
  }
  let name = '$.';
  for (let i = 0; i < components.length - 1; ++i) {
    if (Number.isInteger(components[i])) {
      name += `[${components[i]}].`;
    } else {
      name += `${components[i]}.`;
    }
  }

  if (Number.isInteger(components[components.length - 1])) {
    name += `[${components[components.length - 1]}]`;
  } else {
    name += `${components[components.length - 1]}`;
  }

  return `JSON_VALUE(components[0], '${name}')`;
};

const transformAggregateField = fieldName => {
  if (typeof fieldName !== 'string') {
    return fieldName;
  }
  if (fieldName === '$_created_at') {
    return 'createdAt';
  }
  if (fieldName === '$_updated_at') {
    return 'updatedAt';
  }
  return fieldName.substring(1);
};

const validateKeys = object => {
  if (typeof object == 'object') {
    for (const key in object) {
      if (typeof object[key] == 'object') {
        validateKeys(object[key]);
      }

      if (key.includes('$') || key.includes('.')) {
        throw new Parse.Error(
          Parse.Error.INVALID_NESTED_KEY,
          "Nested keys should not contain the '$' or '.' characters"
        );
      }
    }
  }
};

// Returns the list of join tables on a schema
const joinTablesForSchema = schema => {
  const list = [];
  if (schema) {
    Object.keys(schema.fields).forEach(field => {
      if (schema.fields[field].type === 'Relation') {
        list.push(`_Join:${field}:${schema.className}`);
      }
    });
  }
  return list;
};

interface WhereClause {
  pattern: string;
  binds: { [string]: any };
  sorts: Array<string>;
}

const buildWhereClause = ({ schema, query, caseInsensitive, startBindIndex = 0, className = null }): WhereClause => {
  const patterns = [];
  const binds = {};
  const sorts = [];
  let bindIndex = startBindIndex;

  const getBindName = (prefix = 'p') => {
    return `${prefix}${bindIndex++}`;
  };

  schema = toOracleSchema(schema);

  for (const fieldName in query) {
    const isArrayField =
      schema.fields && schema.fields[fieldName] && schema.fields[fieldName].type === 'Array';
    const initialPatternsLength = patterns.length;
    const fieldValue = query[fieldName];

    // objectId is always a valid field, even if not in schema
    const isObjectId = fieldName === 'objectId';
    
    // nothing in the schema, it's gonna blow up
    if (!schema.fields[fieldName] && !isObjectId) {
      // as it won't exist
      if (fieldValue && fieldValue.$exists === false) {
        continue;
      }
    }

    const authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
    if (authDataMatch) {
      // TODO: Handle querying by _auth_data_provider, authData is stored in authData field
      continue;
    } else if (caseInsensitive && (fieldName === 'username' || fieldName === 'email')) {
      const valueParam = getBindName('val');
      patterns.push(`LOWER("${fieldName}") = LOWER(:${valueParam})`);
      binds[valueParam] = fieldValue;
    } else if (fieldName.indexOf('.') >= 0) {
      let name = transformDotField(fieldName);
      if (fieldValue === null) {
        patterns.push(`"${name}" IS NULL`);
        continue;
      } else {
        if (fieldValue.$in) {
          name = transformDotFieldToComponents(fieldName).join('.');
          const valueParam = getBindName('val');
          patterns.push(`JSON_EXISTS(${name}, '$[*]?(@.value == $${valueParam})')`);
          binds[valueParam] = JSON.stringify(fieldValue.$in);
        } else if (fieldValue.$regex) {
          // Handle later
        } else if (typeof fieldValue !== 'object') {
          const valueParam = getBindName('val');
          patterns.push(`"${name}" = :${valueParam}`);
          binds[valueParam] = fieldValue;
        }
      }
    } else if (fieldValue === null || fieldValue === undefined) {
      patterns.push(`"${fieldName}" IS NULL`);
      continue;
    } else if (typeof fieldValue === 'string') {
      const valueParam = getBindName('val');
      patterns.push(`"${fieldName}" = :${valueParam}`);
      binds[valueParam] = fieldValue;
    } else if (typeof fieldValue === 'boolean') {
      const valueParam = getBindName('val');
      patterns.push(`"${fieldName}" = :${valueParam}`);
      // Can't cast boolean to number
      if (schema.fields[fieldName] && schema.fields[fieldName].type === 'Number') {
        // Should always return zero results
        binds[valueParam] = 9223372036854775808;
      } else {
        binds[valueParam] = fieldValue ? 1 : 0;
      }
    } else if (typeof fieldValue === 'number') {
      const valueParam = getBindName('val');
      patterns.push(`"${fieldName}" = :${valueParam}`);
      binds[valueParam] = fieldValue;
    } else if (['$or', '$nor', '$and'].includes(fieldName)) {
      const clauses = [];
      let currentBindIndex = bindIndex;
      fieldValue.forEach(subQuery => {
        const clause = buildWhereClause({
          schema,
          query: subQuery,
          caseInsensitive,
          startBindIndex: currentBindIndex,
        });
        if (clause.pattern.length > 0) {
          clauses.push(clause.pattern);
          // Merge binds from recursive call
          for (const key in clause.binds) {
            binds[key] = clause.binds[key];
          }
          // Update currentBindIndex to account for the binds used in this clause
          const bindKeys = Object.keys(clause.binds);
          if (bindKeys.length > 0) {
            const bindNumbers = bindKeys.map(k => {
              const match = k.match(/^[a-z]+(\d+)$/);
              return match ? parseInt(match[1]) : -1;
            }).filter(n => n >= 0);
            if (bindNumbers.length > 0) {
              const maxBindNum = Math.max(...bindNumbers);
              currentBindIndex = Math.max(currentBindIndex, maxBindNum + 1);
            }
          }
        }
      });
      bindIndex = currentBindIndex;

      const orOrAnd = fieldName === '$and' ? ' AND ' : ' OR ';
      const not = fieldName === '$nor' ? ' NOT ' : '';

      patterns.push(`${not}(${clauses.join(orOrAnd)})`);
    }

    if (fieldValue.$ne !== undefined) {
      if (isArrayField) {
        const valueParam = getBindName('val');
        fieldValue.$ne = JSON.stringify([fieldValue.$ne]);
        patterns.push(`NOT array_contains(${fieldName}, :${valueParam})`);
        binds[valueParam] = fieldValue.$ne;
      } else {
        if (fieldValue.$ne === null) {
          patterns.push(`"${fieldName}" IS NOT NULL`);
          continue;
        } else {
          // if not null, we need to manually exclude null
          if (fieldValue.$ne.__type === 'GeoPoint') {
            const lonParam = getBindName('lon');
            const latParam = getBindName('lat');
            patterns.push(
              `("${fieldName}" <> SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL) OR "${fieldName}" IS NULL)`
            );
            binds[lonParam] = fieldValue.$ne.longitude;
            binds[latParam] = fieldValue.$ne.latitude;
          } else {
            if (fieldName.indexOf('.') >= 0) {
              const castType = toOracleValueCastType(fieldValue.$ne);
              const constraintFieldName = castType
                ? `CAST("${transformDotField(fieldName)}" AS ${castType})`
                : transformDotField(fieldName);
              const valueParam = getBindName('val');
              patterns.push(
                `(${constraintFieldName} <> :${valueParam} OR ${constraintFieldName} IS NULL)`
              );
              binds[valueParam] = fieldValue.$ne;
            } else if (typeof fieldValue.$ne === 'object' && fieldValue.$ne.$relativeTime) {
              throw new Parse.Error(
                Parse.Error.INVALID_JSON,
                '$relativeTime can only be used with the $lt, $lte, $gt, and $gte operators'
              );
            } else {
              const valueParam = getBindName('val');
              patterns.push(`("${fieldName}" <> :${valueParam} OR "${fieldName}" IS NULL)`);
              binds[valueParam] = fieldValue.$ne;
            }
          }
        }
      }
    }

    if (fieldValue.$eq !== undefined) {
      if (fieldValue.$eq === null) {
        patterns.push(`"${fieldName}" IS NULL`);
      } else {
        if (fieldName.indexOf('.') >= 0) {
          const castType = toOracleValueCastType(fieldValue.$eq);
          const constraintFieldName = castType
            ? `CAST("${transformDotField(fieldName)}" AS ${castType})`
            : `"${transformDotField(fieldName)}"`;
          const valueParam = getBindName('val');
          patterns.push(`${constraintFieldName} = :${valueParam}`);
          binds[valueParam] = fieldValue.$eq;
        } else if (typeof fieldValue.$eq === 'object' && fieldValue.$eq.$relativeTime) {
          throw new Parse.Error(
            Parse.Error.INVALID_JSON,
            '$relativeTime can only be used with the $lt, $lte, $gt, and $gte operators'
          );
        } else {
          const valueParam = getBindName('val');
          patterns.push(`${fieldName} = :${valueParam}`);
          binds[valueParam] = fieldValue.$eq;
        }
      }
    }

    const isInOrNin = Array.isArray(fieldValue.$in) || Array.isArray(fieldValue.$nin);

    if (
      Array.isArray(fieldValue.$in) &&
      isArrayField &&
      schema.fields[fieldName].contents &&
      schema.fields[fieldName].contents.type === 'String'
    ) {
      // For array fields with $in, we need to check if the array contains any of the values
      // Use JSON_TABLE to check if any array element matches the value
      const inPatterns = [];
      let allowNull = false;

      fieldValue.$in.forEach((listElem, index) => {
        if (listElem === null) {
          allowNull = true;
        } else {
          // Since array_contains function is invalid and JSON_TABLE in subqueries doesn't work,
          // use a simpler approach: check if the JSON array string contains the value as a substring
          // This is a workaround - it's not perfect but should work for simple string values
          // We check if the JSON array contains the value by looking for it in the string representation
          // Format: ["value"] - we need to match the value within the array
          // Escape special characters in the value for SQL LIKE patterns
          // For SQL LIKE, we need to escape: % _ and single quotes
          // Note: We don't create bind variables since we're embedding the value directly in the SQL
          const sqlEscapedValue = String(listElem)
            .replace(/\\/g, '\\\\')  // Escape backslashes first
            .replace(/'/g, "''")     // Escape single quotes for SQL (double them)
            .replace(/%/g, '\\%')    // Escape % for LIKE
            .replace(/_/g, '\\_');   // Escape _ for LIKE
          // Use LIKE to check if the JSON array contains the value
          // This checks if the array contains the value as a JSON string element
          // Pattern: ["value"] or [...,"value",...] or ["value",...] or [...,"value"]
          // We use ESCAPE '\' to handle the escaped characters
          inPatterns.push(`("${fieldName}" IS NOT NULL AND "${fieldName}" LIKE '%"${sqlEscapedValue}"%' ESCAPE '\\')`);
        }
      });

      if (allowNull) {
        patterns.push(`("${fieldName}" IS NULL OR ${inPatterns.join(' OR ')})`);
      } else {
        patterns.push(`(${inPatterns.join(' OR ')})`);
      }
    } else if (isInOrNin) {
      const createConstraint = (baseArray, notIn) => {
        const not = notIn ? ' NOT' : '';
        if (baseArray.length > 0) {
          if (isArrayField) {
            const valueParam = getBindName('val');
            patterns.push(`${not} array_contains(${fieldName}, :${valueParam})`);
            binds[valueParam] = JSON.stringify(baseArray);
          } else {
            // Handle Nested Dot Notation Above
            if (fieldName.indexOf('.') >= 0) {
              return;
            }
            const inPatterns = [];
            baseArray.forEach(listElem => {
              if (listElem != null) {
                const valueParam = getBindName('val');
                binds[valueParam] = listElem;
                inPatterns.push(`:${valueParam}`);
              }
            });
            patterns.push(`"${fieldName}"${not} IN (${inPatterns.join(',')})`);
          }
        } else if (!notIn) {
          patterns.push(`"${fieldName}" IS NULL`);
        } else {
          // Handle empty array
          if (notIn) {
            patterns.push('1 = 1'); // Return all values
          } else {
            patterns.push('1 = 0'); // Return no values
          }
        }
      };

      if (fieldValue.$in) {
        createConstraint(
          _.flatMap(fieldValue.$in, elt => elt),
          false
        );
      }
      if (fieldValue.$nin) {
        createConstraint(
          _.flatMap(fieldValue.$nin, elt => elt),
          true
        );
      }
    } else if (typeof fieldValue.$in !== 'undefined') {
      throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $in value');
    } else if (typeof fieldValue.$nin !== 'undefined') {
      throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $nin value');
    }

    if (Array.isArray(fieldValue.$all) && isArrayField) {
      const valueParam = getBindName('val');
      if (isAnyValueRegexStartsWith(fieldValue.$all)) {
        if (!isAllValuesRegexOrNone(fieldValue.$all)) {
          throw new Parse.Error(
            Parse.Error.INVALID_JSON,
            'All $all values must be of regex type or none: ' + fieldValue.$all
          );
        }

        for (let i = 0; i < fieldValue.$all.length; i += 1) {
          const value = processRegexPattern(fieldValue.$all[i].$regex);
          fieldValue.$all[i] = value.substring(1) + '%';
        }
        patterns.push(`array_contains_all_regex(${fieldName}, :${valueParam})`);
      } else {
        patterns.push(`array_contains_all(${fieldName}, :${valueParam})`);
      }
      binds[valueParam] = JSON.stringify(fieldValue.$all);
    } else if (Array.isArray(fieldValue.$all)) {
      if (fieldValue.$all.length === 1) {
        const valueParam = getBindName('val');
        patterns.push(`"${fieldName}" = :${valueParam}`);
        binds[valueParam] = fieldValue.$all[0].objectId;
      }
    }

    if (typeof fieldValue.$exists !== 'undefined') {
      if (typeof fieldValue.$exists === 'object' && fieldValue.$exists.$relativeTime) {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          '$relativeTime can only be used with the $lt, $lte, $gt, and $gte operators'
        );
      } else if (fieldValue.$exists) {
        patterns.push(`"${fieldName}" IS NOT NULL`);
      } else {
        patterns.push(`"${fieldName}" IS NULL`);
      }
    }

    if (fieldValue.$containedBy) {
      const arr = fieldValue.$containedBy;
      if (!(arr instanceof Array)) {
        throw new Parse.Error(Parse.Error.INVALID_JSON, `bad $containedBy: should be an array`);
      }
      const valueParam = getBindName('val');
      // Oracle: использование JSON_QUERY для проверки вхождения
      patterns.push(`JSON_EXISTS("${fieldName}", '$[*]?(@.value in ($${valueParam}))')`);
      binds[valueParam] = JSON.stringify(arr);
    }

    if (fieldValue.$text) {
      const search = fieldValue.$text.$search;
      if (typeof search !== 'object') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, `bad $text: $search, should be object`);
      }
      if (!search.$term || typeof search.$term !== 'string') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, `bad $text: $term, should be string`);
      }
      if (search.$language && typeof search.$language !== 'string') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, `bad $text: $language, should be string`);
      }
      if (search.$caseSensitive && typeof search.$caseSensitive !== 'boolean') {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          `bad $text: $caseSensitive, should be boolean`
        );
      } else if (search.$caseSensitive) {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          `bad $text: $caseSensitive not supported, please use $regex or create a separate lower case column.`
        );
      }
      if (search.$diacriticSensitive && typeof search.$diacriticSensitive !== 'boolean') {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          `bad $text: $diacriticSensitive, should be boolean`
        );
      }

      const termParam = getBindName('term');
      // Oracle Text: CONTAINS operator
      patterns.push(`CONTAINS("${fieldName}", :${termParam}) > 0`);
      binds[termParam] = search.$term;
    }

    if (fieldValue.$nearSphere) {
      const point = fieldValue.$nearSphere;
      const distance = fieldValue.$maxDistance;
      const distanceInKM = distance * 6371 * 1000;

      const lonParam = getBindName('lon');
      const latParam = getBindName('lat');
      const distParam = getBindName('dist');

      // Oracle Spatial: SDO_GEOM.SDO_DISTANCE
      patterns.push(
        `SDO_GEOM.SDO_DISTANCE(${fieldName}, SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL), 0.005) <= :${distParam}`
      );
      sorts.push(
        `SDO_GEOM.SDO_DISTANCE(${fieldName}, SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL), 0.005) ASC`
      );
      binds[lonParam] = point.longitude;
      binds[latParam] = point.latitude;
      binds[distParam] = distanceInKM;
    }

    if (fieldValue.$within && fieldValue.$within.$box) {
      const box = fieldValue.$within.$box;
      const left = box[0].longitude;
      const bottom = box[0].latitude;
      const right = box[1].longitude;
      const top = box[1].latitude;

      const boxParam = getBindName('box');
      patterns.push(
        `SDO_RELATE("${fieldName}", SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1,1003,3), SDO_ORDINATE_ARRAY(:${boxParam}_minx, :${boxParam}_miny, :${boxParam}_maxx, :${boxParam}_maxy)), 'mask=INSIDE') = 'TRUE'`
      );
      binds[`${boxParam}_minx`] = left;
      binds[`${boxParam}_miny`] = bottom;
      binds[`${boxParam}_maxx`] = right;
      binds[`${boxParam}_maxy`] = top;
    }

    if (fieldValue.$geoWithin && fieldValue.$geoWithin.$centerSphere) {
      const centerSphere = fieldValue.$geoWithin.$centerSphere;
      if (!(centerSphere instanceof Array) || centerSphere.length < 2) {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          'bad $geoWithin value; $centerSphere should be an array of Parse.GeoPoint and distance'
        );
      }

      let point = centerSphere[0];
      if (point instanceof Array && point.length === 2) {
        point = new Parse.GeoPoint(point[1], point[0]);
      } else if (!GeoPointCoder.isValidJSON(point)) {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          'bad $geoWithin value; $centerSphere geo point invalid'
        );
      }
      Parse.GeoPoint._validate(point.latitude, point.longitude);

      const distance = centerSphere[1];
      if (isNaN(distance) || distance < 0) {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          'bad $geoWithin value; $centerSphere distance invalid'
        );
      }
      const distanceInKM = distance * 6371 * 1000;

      const lonParam = getBindName('lon');
      const latParam = getBindName('lat');
      const distParam = getBindName('dist');

      patterns.push(
        `SDO_GEOM.SDO_DISTANCE(${fieldName}, SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL), 0.005) <= :${distParam}`
      );
      binds[lonParam] = point.longitude;
      binds[latParam] = point.latitude;
      binds[distParam] = distanceInKM;
    }

    if (fieldValue.$geoWithin && fieldValue.$geoWithin.$polygon) {
      const polygon = fieldValue.$geoWithin.$polygon;
      let points;

      if (typeof polygon === 'object' && polygon.__type === 'Polygon') {
        if (!polygon.coordinates || polygon.coordinates.length < 3) {
          throw new Parse.Error(
            Parse.Error.INVALID_JSON,
            'bad $geoWithin value; Polygon.coordinates should contain at least 3 lon/lat pairs'
          );
        }
        points = polygon.coordinates;
      } else if (polygon instanceof Array) {
        if (polygon.length < 3) {
          throw new Parse.Error(
            Parse.Error.INVALID_JSON,
            'bad $geoWithin value; $polygon should contain at least 3 GeoPoints'
          );
        }
        points = polygon;
      } else {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          "bad $geoWithin value; $polygon should be Polygon object or Array of Parse.GeoPoint's"
        );
      }

      const ordinates = [];
      points.forEach(point => {
        if (point instanceof Array && point.length === 2) {
          Parse.GeoPoint._validate(point[1], point[0]);
          ordinates.push(point[0], point[1]);
        } else if (typeof point === 'object' && point.__type === 'GeoPoint') {
          Parse.GeoPoint._validate(point.latitude, point.longitude);
          ordinates.push(point.longitude, point.latitude);
        } else {
          throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $geoWithin value');
        }
      });

      const polyParam = getBindName('poly');
      const ordinateBinds = ordinates.map((_, i) => `:${polyParam}_${i}`).join(',');
      ordinates.forEach((ord, i) => {
        binds[`${polyParam}_${i}`] = ord;
      });

      patterns.push(
        `SDO_RELATE("${fieldName}", SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1,1003,1), SDO_ORDINATE_ARRAY(${ordinateBinds})), 'mask=INSIDE') = 'TRUE'`
      );
    }

    if (fieldValue.$geoIntersects && fieldValue.$geoIntersects.$point) {
      const point = fieldValue.$geoIntersects.$point;
      if (typeof point !== 'object' || point.__type !== 'GeoPoint') {
        throw new Parse.Error(
          Parse.Error.INVALID_JSON,
          'bad $geoIntersect value; $point should be GeoPoint'
        );
      }
      Parse.GeoPoint._validate(point.latitude, point.longitude);

      const lonParam = getBindName('lon');
      const latParam = getBindName('lat');

      patterns.push(
        `SDO_RELATE(${fieldName}, SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL), 'mask=CONTAINS') = 'TRUE'`
      );
      binds[lonParam] = point.longitude;
      binds[latParam] = point.latitude;
    }

    if (fieldValue.$regex) {
      let regex = fieldValue.$regex;
      const opts = fieldValue.$options;
      let regexOpts = '';

      if (opts) {
        if (opts.indexOf('i') >= 0) {
          regexOpts += 'i';
        }
        if (opts.indexOf('x') >= 0) {
          regex = removeWhiteSpace(regex);
        }
      }

      const name = transformDotField(fieldName);
      regex = processRegexPattern(regex);

      const regexParam = getBindName('regex');
      if (regexOpts) {
        const optsParam = getBindName('opts');
        patterns.push(`REGEXP_LIKE("${name}", :${regexParam}, :${optsParam})`);
        binds[regexParam] = regex;
        binds[optsParam] = regexOpts;
      } else {
        patterns.push(`REGEXP_LIKE("${name}", :${regexParam})`);
        binds[regexParam] = regex;
      }
    }

    if (fieldValue.__type === 'Pointer') {
      const valueParam = getBindName('val');
      if (isArrayField) {
        patterns.push(`array_contains("${fieldName}", :${valueParam})`);
        binds[valueParam] = JSON.stringify([fieldValue]);
      } else {
        patterns.push(`"${fieldName}" = :${valueParam}`);
        binds[valueParam] = fieldValue.objectId;
      }
    }

    if (fieldValue.__type === 'Date') {
      const valueParam = getBindName('val');
      patterns.push(
        `${fieldName} = TO_TIMESTAMP(:${valueParam}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')`
      );
      binds[valueParam] = fieldValue.iso;
    }

    if (fieldValue.__type === 'GeoPoint') {
      const lonParam = getBindName('lon');
      const latParam = getBindName('lat');
      patterns.push(
        `SDO_GEOM.SDO_DISTANCE("${fieldName}", SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonParam}, :${latParam}, NULL), NULL, NULL), 0.005) < 0.001`
      );
      binds[lonParam] = fieldValue.longitude;
      binds[latParam] = fieldValue.latitude;
    }

    if (fieldValue.__type === 'Polygon') {
      const value = convertPolygonToSQL(fieldValue.coordinates);
      const valueParam = getBindName('val');
      patterns.push(`${fieldName} = :${valueParam}`);
      binds[valueParam] = value;
    }

    Object.keys(ParseToOracleComparator).forEach(cmp => {
      if (fieldValue[cmp] || fieldValue[cmp] === 0) {
        const oracleComparator = ParseToOracleComparator[cmp];
        let constraintFieldName;
        let oracleValue = toOracleValue(fieldValue[cmp]);

        if (fieldName.indexOf('.') >= 0) {
          const castType = toOracleValueCastType(fieldValue[cmp]);
          constraintFieldName = castType
            ? `CAST("${transformDotField(fieldName)}" AS ${castType})`
            : transformDotField(fieldName);
        } else {
          if (typeof oracleValue === 'object' && oracleValue.$relativeTime) {
            if (schema.fields[fieldName].type !== 'Date') {
              throw new Parse.Error(
                Parse.Error.INVALID_JSON,
                '$relativeTime can only be used with Date field'
              );
            }
            const parserResult = Utils.relativeTimeToDate(oracleValue.$relativeTime);
            if (parserResult.status === 'success') {
              oracleValue = toOracleValue(parserResult.result);
            } else {
              console.error('Error while parsing relative date', parserResult);
              throw new Parse.Error(
                Parse.Error.INVALID_JSON,
                `bad $relativeTime (${oracleValue.$relativeTime}) value. ${parserResult.info}`
              );
            }
          }
          constraintFieldName = fieldName;
        }
        const valueParam = getBindName('val');
        binds[valueParam] = oracleValue;
        patterns.push(`${constraintFieldName} ${oracleComparator} :${valueParam}`);
      }
    });

    if (initialPatternsLength === patterns.length) {
      throw new Parse.Error(
        Parse.Error.OPERATION_FORBIDDEN,
        `Oracle doesn't support this query type yet ${JSON.stringify(fieldValue)}`
      );
    }
  }

  Object.keys(binds).forEach(key => {
    binds[key] = transformValue(binds[key]);
  });

  return { pattern: patterns.join(' AND '), binds, sorts };
};

export class OracleStorageAdapter implements StorageAdapter {
  canSortOnJoinTables: boolean;
  enableSchemaHooks: boolean;

  // Private
  _collectionPrefix: string;
  _client: any;
  _onchange: any;
  _pgp: any;
  _stream: any;
  _uuid: any;
  _connectOptions: object;
  _pollingConnection: any;
  _isPolling: boolean;
  schemaCacheTtl: ?number;

  constructor({ uri, collectionPrefix = '', databaseOptions = {} }: any) {
    const options = { ...databaseOptions };
    this._collectionPrefix = collectionPrefix;
    this.enableSchemaHooks = !!databaseOptions.enableSchemaHooks;
    this.schemaCacheTtl = databaseOptions.schemaCacheTtl;
    for (const key of ['enableSchemaHooks', 'schemaCacheTtl']) {
      delete options[key];
    }

    const { pool, orcl, connectOptions } = createClient(uri, options);

    this._client = orcl;
    this._onchange = () => {};
    this._pgp = pool;
    this._connectOptions = connectOptions;
    this._uuid = uuidv4();
    this.canSortOnJoinTables = false;
  }

  watch(callback: () => void): void {
    this._onchange = callback;
  }

  //Note that analyze=true will run the query, executing INSERTS, DELETES, etc.

  _prepareExplainQuery(
    query: string,
    analyze: boolean = false,
    statementId?: string
  ): {
    explainQuery: string,
    fetchQuery: string,
    cleanupQuery: string,
  } {
    const stmtId = statementId || `STMT_${Date.now()}`;
    if (analyze) {
      return {
        explainQuery: `
          ALTER SESSION SET STATISTICS_LEVEL = ALL;
          ${query};
      `.trim(),
        fetchQuery: `
        SELECT DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST +OUTLINE') as plan
        FROM DUAL
      `,
        cleanupQuery: `ALTER SESSION SET STATISTICS_LEVEL = TYPICAL`,
      };
    } else {
      return {
        explainQuery: `EXPLAIN PLAN SET STATEMENT_ID = '${stmtId}' FOR ${query}`,
        fetchQuery: `
            SELECT 
              JSON_OBJECT(
                'operation' VALUE operation,
                'options' VALUE options,
                'object_name' VALUE object_name,
                'object_type' VALUE object_type,
                'cost' VALUE cost,
                'cardinality' VALUE cardinality,
                'bytes' VALUE bytes,
                'cpu_cost' VALUE cpu_cost,
                'io_cost' VALUE io_cost,
                'access_predicates' VALUE access_predicates,
                'filter_predicates' VALUE filter_predicates
              ) as plan_json
            FROM PLAN_TABLE 
            WHERE STATEMENT_ID = '${stmtId}'
            ORDER BY id
        `,
        cleanupQuery: `DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '${stmtId}'`,
      };
    }
  }

  createExplainableQuery(query: string): string {
    const explainQuery = this._prepareExplainQuery(query, false);
    return explainQuery.explainQuery;
  }

  async explainQuery(connection: any, query: string, analyze: boolean = false): Promise<any> {
    const explainQuery = this._prepareExplainQuery(query, analyze);

    try {
      await connection.execute(explainQuery.explainQuery);

      const result = await connection.execute(
        explainQuery.fetchQuery,
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      if (explainQuery.cleanupQuery) {
        await connection.execute(explainQuery.cleanupQuery);
      }

      return result.rows.map(row => row.PLAN_TABLE_OUTPUT || row.plan).join('\n');
    } catch (error) {
      if (explainQuery.cleanupQuery) {
        try {
          await connection.execute(explainQuery.cleanupQuery);
        } catch (cleanupError) {
          console.error('Cleanup error:', cleanupError);
        }
      }
      throw error;
    }
  }
  async handleShutdown() {
    const stream = this._stream;
    if (stream && stream.type === 'polling' && stream.interval) {
      clearInterval(stream.interval);
    }
    if (this._pollingInterval) {
      clearInterval(this._pollingInterval);
      this._pollingInterval = null;
    }
    if (this._schemaPollingInterval) {
      clearInterval(this._schemaPollingInterval);
      this._schemaPollingInterval = null;
    }
    if (this._schemaChangeInterval) {
      clearInterval(this._schemaChangeInterval);
      this._schemaChangeInterval = null;
    }

    let attempts = 0;
    while (this._isPolling && attempts < 50) {
      await new Promise(resolve => setTimeout(resolve, 100));
      attempts++;
    }

    if (this._pollingConnection) {
      try {
        await this._pollingConnection.close();
      } catch (error) {
        console.error('Error closing polling connection:', error);
      }
      this._pollingConnection = null;
    }

    this._stream = null;
    this._isPolling = false;

    // Close connection pool if it was created
    try {
      const pool = await this._pgp;
      if (pool && typeof pool.close === 'function') {
        await pool.close();
      }
    } catch (e) {
      console.error('Error closing Oracle pool:', e);
    }
  }

  async _listenToSchema() {
    await this._pgp;
    if (this._stream || !this.enableSchemaHooks) {
      return;
    }

    try {
      const pool = await this._pgp;
      const setupConn = await pool.getConnection();
      try {
        await setupConn.execute(`
        DECLARE
          table_exists NUMBER;
        BEGIN
          SELECT COUNT(*) INTO table_exists 
          FROM USER_TABLES 
          WHERE TABLE_NAME = '_SchemaChangeLog';
          
          IF table_exists = 0 THEN
            EXECUTE IMMEDIATE 'CREATE TABLE "_SchemaChangeLog" (
              "senderId" VARCHAR2(100) PRIMARY KEY,
              "lastChange" TIMESTAMP NOT NULL
            )';
          END IF;
        END;
      `);
      } finally {
        await setupConn.close();
      }

      // Initialize to a time slightly in the past to ensure we catch changes that happen immediately
      this._lastKnownChange = new Date(Date.now() - 10000);
      this._isPolling = false;

      this._pollingConnection = await pool.getConnection();

      // Set _stream before starting the interval to ensure _notifySchemaChange can work
      this._stream = { polling: true };

      this._schemaChangeInterval = setInterval(async () => {
        if (this._isPolling) {
          console.log('Previous poll still running, skipping...');
          return;
        }

        this._isPolling = true;

        try {
          const result = await this._pollingConnection.execute(
            `SELECT "senderId", "lastChange"
           FROM "_SchemaChangeLog"
           WHERE "lastChange" > :lastCheck
             AND "senderId" != :uuid`,
            {
              lastCheck: this._lastKnownChange,
              uuid: this._uuid,
            },
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
          );
          if (result.rows && result.rows.length > 0) {
            this._onchange();

            this._lastKnownChange = result.rows.reduce((latest, row) => {
              const rowTime = new Date(row.lastChange);
              return rowTime > latest ? rowTime : latest;
            }, this._lastKnownChange);
          }
        } catch (error) {
          console.error('Error in schema polling:', error);
        } finally {
          this._isPolling = false;
        }
      }, 2000);
      console.log('Schema polling started successfully');
    } catch (error) {
      console.error('Failed to setup schema listener:', error);
      await this._cleanupSchemaListener();
      throw error;
    }
  }

  async _cleanupSchemaListener() {
    console.log('Cleaning up schema listener...');

    if (this._schemaChangeInterval) {
      clearInterval(this._schemaChangeInterval);
      this._schemaChangeInterval = null;
    }

    let attempts = 0;
    while (this._isPolling && attempts < 50) {
      await new Promise(resolve => setTimeout(resolve, 100));
      attempts++;
    }

    if (this._pollingConnection) {
      try {
        await this._pollingConnection.close();
      } catch (error) {
        console.error('Error closing polling connection:', error);
      }
      this._pollingConnection = null;
    }

    this._stream = null;
    this._isPolling = false;

    console.log('Schema listener cleaned up');
  }

  async _notifySchemaChange() {
    if (!this._stream) return;

    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      // Сначала пробуем UPDATE
      const result = await connection.execute(`
      UPDATE "_SchemaChangeLog"
      SET "lastChange" = SYSTIMESTAMP
      WHERE "senderId" = :senderId
    `, {
        senderId: this._uuid
      });

      // Если запись не существует, делаем INSERT
      if (result.rowsAffected === 0) {
        try {
          await connection.execute(`
          INSERT INTO "_SchemaChangeLog" ("senderId", "lastChange")
          VALUES (:senderId, SYSTIMESTAMP)
        `, {
            senderId: this._uuid
          });
        } catch (insertError) {
          // Игнорируем ошибку дубликата - другой процесс вставил одновременно
          if (insertError.errorNum !== 1) {
            throw insertError;
          }
        }
      }

      await connection.commit();
    } catch (error) {
      console.log('Failed to notify schema change:', error);
    } finally {
      await connection.close();
    }
  }
  async _setDateFormats() {
    const pool = await this._pgp;
    const connection = await pool.getConnection();
    await connection.execute(`
    ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'
  `);

    await connection.execute(`
    ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD/MM/YYYY HH24:MI:SS.FF'
  `);

    await connection.execute(`
    ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'DD/MM/YYYY HH24:MI:SS.FF TZR'
  `);

    debug('Date formats configured');

    await connection.close();
  }

  async _ensureSchemaCollectionExists(conn: any) {
    const shouldCloseConnection = !conn;
    const pool = await this._pgp;
    conn = conn || (await pool.getConnection());
    await conn
      .execute(
        'CREATE TABLE IF NOT EXISTS "_SCHEMA" (\n' +
          '      "className" VARCHAR2(120),\n' +
          '      "schema" JSON,\n' +
          '      "isParseClass" NUMBER(1),\n' +
          '      CONSTRAINT "_SCHEMA_PK" PRIMARY KEY ("className")\n' +
          '    )'
      )
      .catch(error => {
        throw error;
      });

    if (shouldCloseConnection) {
      conn.close();
    }
  }

  async classExists(name: string): Promise<boolean> {
    const sql = `
    SELECT COUNT(*) as cnt
    FROM user_tables
    WHERE table_name = :tableName
  `;
    const pool = await this._pgp;
    const connection = await pool.getConnection();
    const result = await connection.execute(
      sql,
      { tableName: name.toUpperCase() },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (connection) {
      await connection.close();
    }

    return result.rows[0].CNT > 0;
  }

  async setClassLevelPermissions(className: string, CLPs: any): Promise<void> {
    const sql = `
    UPDATE "_SCHEMA"
    SET "schema" = JSON_TRANSFORM(
      "schema",
      SET '$.classLevelPermissions' = :clps
    )
    WHERE "className" = :className
  `;

    let connection = null;
    try {
    const pool = await this._pgp;
    connection = await pool.getConnection();
      const result = await connection.execute(
        sql,
        {
          className: className,
          clps: JSON.stringify(CLPs),
        },
        { autoCommit: true }
      );

      if (result.rowsAffected === 0) {
        throw new Error(`Class ${className} not found in schema`);
      }
      this._notifySchemaChange();
    } catch (error) {
      console.error('Error setting class level permissions:', error);
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close(); // always release the connection back to the pool
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  async setIndexesWithSchemaFormat(
    className: string,
    submittedIndexes: any,
    existingIndexes: any = {},
    fields: any,
    conn?: any
  ): Promise<void> {
    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      if (submittedIndexes === undefined) {
        return Promise.resolve();
      }

      if (Object.keys(existingIndexes).length === 0) {
        existingIndexes = { _id_: { _id: 1 } };
      }

      const deletedIndexes = [];
      const insertedIndexes = [];

      Object.keys(submittedIndexes).forEach(name => {
        const field = submittedIndexes[name];

        if (existingIndexes[name] && field.__op !== 'Delete') {
          throw new Parse.Error(Parse.Error.INVALID_QUERY, `Index ${name} exists, cannot update.`);
        }

        if (!existingIndexes[name] && field.__op === 'Delete') {
          throw new Parse.Error(
            Parse.Error.INVALID_QUERY,
            `Index ${name} does not exist, cannot delete.`
          );
        }

        if (field.__op === 'Delete') {
          deletedIndexes.push(name);
          delete existingIndexes[name];
        } else {
          Object.keys(field).forEach(key => {
            if (!Object.prototype.hasOwnProperty.call(fields, key)) {
              throw new Parse.Error(
                Parse.Error.INVALID_QUERY,
                `Field ${key} does not exist, cannot add index.`
              );
            }
          });

          existingIndexes[name] = field;
          insertedIndexes.push({
            key: field,
            name,
          });
        }
      });

      try {
        if (insertedIndexes.length > 0) {
          await this.createIndexes(className, insertedIndexes, connection);
        }

        if (deletedIndexes.length > 0) {
          await this.dropIndexes(className, deletedIndexes, connection);
        }

        const updateSchemaSql = `
        UPDATE "_SCHEMA"
        SET "schema" = JSON_MERGEPATCH(
          "schema",
          JSON_OBJECT('indexes' VALUE :indexes FORMAT JSON)
        )
        WHERE "className" = :className
      `;

        const result = await connection.execute(updateSchemaSql, {
          className: className,
          indexes: JSON.stringify(existingIndexes),
        });

        if (result.rowsAffected === 0) {
          throw new Error(`Class ${className} not found in schema`);
        }

        await connection.commit();

        this._notifySchemaChange();
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async createClass(className: string, schema: SchemaType, conn?: any) {
    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      await this.createTable(className, schema, connection);

      const insertSql = `
      INSERT INTO "_SCHEMA" ("className", "schema", "isParseClass")
      VALUES (:className, :schema, 1)
    `;

      await connection.execute(insertSql, {
        className: className,
        schema: JSON.stringify(schema),
      });

      await this.setIndexesWithSchemaFormat(
        className,
        schema.indexes,
        {},
        schema.fields,
        connection
      );

      if (shouldCloseConnection) {
        await connection.commit();
      }

      this._notifySchemaChange();

      return toParseSchema(schema);
    } catch (err) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }

      if (err.errorNum === 1) {
        const errorMessage = err.message || '';
        if (errorMessage.includes(className) || errorMessage.includes('_SCHEMA_PK')) {
          throw new Parse.Error(Parse.Error.DUPLICATE_VALUE, `Class ${className} already exists.`);
        }
      }

      throw err;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  // Just create a table, do not insert in schema
  async createTable(className: string, schema: SchemaType, conn: any) {
    const pool = await this._pgp;
    const shouldCloseConnection = !conn;
    const connection = conn || (await pool.getConnection());

    debug('createTable', className);

    const fields = Object.assign({}, schema.fields);

    if (className === '_User') {
      fields._email_verify_token_expires_at = { type: 'Date' };
      fields._email_verify_token = { type: 'String' };
      fields._account_lockout_expires_at = { type: 'Date' };
      fields._failed_login_count = { type: 'Number' };
      fields._perishable_token = { type: 'String' };
      fields._perishable_token_expires_at = { type: 'Date' };
      fields._password_changed_at = { type: 'Date' };
      fields._password_history = { type: 'Array' };
    }

    const columnDefinitions = [];
    const relations = [];

    Object.keys(fields).forEach(fieldName => {
      const parseType = fields[fieldName];

      if (parseType.type === 'Relation') {
        relations.push(fieldName);
        return;
      }

      if (fieldName === '_rperm' || fieldName === '_wperm') {
        parseType.contents = { type: 'String' };
      }

      const oracleType = parseTypeToOracleType(parseType);

      columnDefinitions.push(`"${fieldName}" ${oracleType}`);

      if (fieldName === 'objectId') {
        columnDefinitions.push(`PRIMARY KEY ("${fieldName}")`);
      }
    });

    const createTableSql = `
      CREATE TABLE IF NOT EXISTS "${className}" (
                                    ${columnDefinitions.join(',\n      ')}
      )
    `;

    try {
      await connection.execute(createTableSql);
    } catch (error) {
      // ORA-00955: name is already used by an existing object
      if (error.errorNum === 955) {
        console.log(`Table ${className} already exists, skipping creation`);
      } else {
        throw error;
      }
    }

    for (const fieldName of relations) {
      const joinTableName = `_Join:${fieldName}:${className}`;
      const createJoinTableSql = `
      CREATE TABLE "${joinTableName}" (
        "relatedId" VARCHAR2(120),
        "owningId" VARCHAR2(120),
        PRIMARY KEY ("relatedId", "owningId")
      )
    `;

      try {
        await connection.execute(createJoinTableSql);
      } catch (error) {
        if (error.errorNum === 955) {
          console.log(`Join table ${joinTableName} already exists, skipping creation`);
        } else {
          throw error;
        }
      }
    }
    if (shouldCloseConnection && connection) {
      await connection.close();
    }
  }

  async schemaUpgrade(className: string, schema: SchemaType, conn: any) {
    debug('schemaUpgrade', className);

    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      const result = await connection.execute(
        `SELECT column_name 
       FROM user_tab_columns 
       WHERE table_name = :tableName`,
        { tableName: className.toUpperCase() },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const existingColumns = result.rows.map(row => row.COLUMN_NAME);

      const newFields = Object.keys(schema.fields).filter(
        fieldName => !existingColumns.includes(fieldName.toUpperCase())
      );

      await Promise.all(
        newFields.map(fieldName =>
          this.addFieldIfNotExists(className, fieldName, schema.fields[fieldName], connection)
        )
      );

      if (shouldCloseConnection) {
        await connection.commit();
      }
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }
  async addFieldIfNotExists(className: string, fieldName: string, type: any, conn?: any) {
    debug('addFieldIfNotExists', className, fieldName);
    await this._pgp;

    return ddlQueue.enqueue(className, async () => {
      const pool = await this._pgp;
      const connection = conn || (await pool.getConnection());
      const shouldCloseConnection = !conn;

      try {
        await connection.execute(`ALTER SESSION SET DDL_LOCK_TIMEOUT = 30`);

        if (type.type !== 'Relation') {
          const oracleType = parseTypeToOracleType(type);
          const alterSql = `DECLARE
            v_count NUMBER;
            BEGIN
              SELECT COUNT(*)
              INTO v_count
              FROM user_tab_cols
              WHERE table_name  = '${className}'
                AND column_name = '${fieldName}';

              IF v_count = 0 THEN
                EXECUTE IMMEDIATE '
                  ALTER TABLE "${className}"
                  ADD "${fieldName}" ${oracleType}
                ';
              END IF;
          END;
         `;

          await retryOnLock(
            async () => {
              try {
                await connection.execute(alterSql);
              } catch (error) {
                // ORA-00942: table or view does not exist
                if (error.errorNum === 942) {
                  await this.createClass(className, { fields: { [fieldName]: type } }, connection);
                  if (shouldCloseConnection) {
                    await connection.commit();
                  }
                  this._notifySchemaChange();
                  return;
                }
                // ORA-01430: column being added already exists in table
                if (error.errorNum !== 1430) {
                  throw error;
                }
              }
            },
            5,
            1000
          );
        } else {
          const joinTableName = `_Join:${fieldName}:${className}`;
          const createJoinSql = `
          CREATE TABLE IF NOT EXISTS "${joinTableName}" (
            "relatedId" VARCHAR2(120),
            "owningId" VARCHAR2(120),
            PRIMARY KEY ("relatedId", "owningId")
          )
        `;

          await ddlQueue.enqueue(joinTableName, async () => {
            await retryOnLock(
              async () => {
                try {
                  await connection.execute(createJoinSql);
                } catch (error) {
                  // ORA-00955: name is already used by an existing object
                  // Oracle 23c IF NOT EXISTS should prevent this, but keep for compatibility
                  if (error.errorNum !== 955) {
                    throw error;
                  }
                }
              },
              5,
              1000
            );
          });
        }

        // Check if field already exists in schema - if so, skip adding it (IF NOT EXISTS behavior)
        const checkSql = `
        SELECT "schema"
        FROM "_SCHEMA"
        WHERE "className" = :className
          AND JSON_EXISTS("schema", '$.fields.${fieldName}')
      `;

        const checkResult = await connection.execute(
          checkSql,
          { className: className },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        if (checkResult.rows.length > 0) {
          // Field already exists in schema - skip adding it (IF NOT EXISTS behavior)
          debug(`Field ${fieldName} already exists in schema for class ${className}, skipping`);
          if (shouldCloseConnection) {
            await connection.commit();
          }
          return;
        }

        const updateSql = `
        UPDATE "_SCHEMA"
        SET "schema" = JSON_MERGEPATCH(
          "schema",
          JSON_OBJECT(
            'fields' VALUE JSON_MERGEPATCH(
              JSON_QUERY("schema", '$.fields'),
              JSON_OBJECT(:fieldName VALUE :fieldType FORMAT JSON)
            ) FORMAT JSON
          )
        )
        WHERE "className" = :className
      `;

        await connection.execute(updateSql, {
          className: className,
          fieldName: fieldName,
          fieldType: JSON.stringify(type),
        });

        if (shouldCloseConnection) {
          await connection.commit();
        }
        this._notifySchemaChange();
      } catch (error) {
        if (shouldCloseConnection) {
          await connection.rollback();
        }
        throw error;
      } finally {
        if (shouldCloseConnection && connection) {
          await connection.close();
        }
      }
    });
  }

  async updateFieldOptions(className: string, fieldName: string, type: any) {
    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      const result = await connection.execute(
        `SELECT "schema" FROM "_SCHEMA" WHERE "className" = :className FOR UPDATE`,
        { className },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const schema = result.rows[0].schema;
      if (!schema.fields) {
        schema.fields = {};
      }
      schema.fields[fieldName] = type;

      await connection.execute(
        `UPDATE "_SCHEMA" SET "schema" = :schema WHERE "className" = :className`,
        { className, schema: JSON.stringify(schema) }
      );

      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      await connection.close();
    }
  }

  // Drops a collection. Resolves with true if it was a Parse Schema (eg. _User, Custom, etc.)
  // and resolves with false if it wasn't (eg. a join table). Rejects if deletion was impossible.
  async deleteClass(className: string) {
    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      const dropTableSql = `DROP TABLE "${className}" CASCADE CONSTRAINTS`;

      try {
        await connection.execute(dropTableSql);
      } catch (error) {
        // ORA-00942: table or view does not exist
        if (error.errorNum !== 942) {
          throw error;
        }
      }

      const deleteSchemaSql = `DELETE FROM "_SCHEMA" WHERE "className" = :className`;

      await connection.execute(deleteSchemaSql, {
        className: className,
      });

      await connection.commit();

      this._notifySchemaChange();

      return className.indexOf('_Join:') !== 0;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      await connection.close();
    }
  }

  // Delete all data known to this adapter. Used for testing.
  async deleteAllClasses() {
    const now = new Date().getTime();
    debug('deleteAllClasses');

    if (this._client?.ended || this._client?._closing) {
      return;
    }

    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      let results;
      try {
        results = await connection.execute(
          `SELECT * FROM "_SCHEMA"`,
          {},
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
      } catch (error) {
        // ORA-00942: table or view does not exist
        if (error.errorNum === 942) {
          debug('_SCHEMA table does not exist, nothing to delete');
          return;
        }
        throw error;
      }

      const joins = results.rows.reduce((list, row) => {
        const schema = row.schema;
        return list.concat(joinTablesForSchema(schema));
      }, []);

      const classes = [
        '_SCHEMA',
        '_PushStatus',
        '_JobStatus',
        '_JobSchedule',
        '_Hooks',
        '_GlobalConfig',
        '_GraphQLConfig',
        '_Audience',
        '_Idempotency',
        ...results.rows.map(row => row.className),
        ...joins,
      ];

      for (const className of classes) {
        try {
          await connection.execute(`DROP TABLE "${className}" CASCADE CONSTRAINTS`);
          debug(`Dropped table: ${className}`);
        } catch (error) {
          if (error.errorNum !== 942) {
            console.warn(`Warning: Could not drop table ${className}:`, error.message);
          }
        }
      }

      await connection.commit();

      debug(`deleteAllClasses done in ${new Date().getTime() - now}ms`);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      await connection.close();
    }
  }

  // Remove the column and all the data. For Relations, the _Join collection is handled
  // specially, this function does not delete _Join columns. It should, however, indicate
  // that the relation fields does not exist anymore. In mongo, this means removing it from
  // the _SCHEMA collection.  There should be no actual data in the collection under the same name
  // as the relation column, so it's fine to attempt to delete it. If the fields listed to be
  // deleted do not exist, this function should return successfully anyways. Checking for
  // attempts to delete non-existent fields is the responsibility of Parse Server.

  // This function is not obligated to delete fields atomically. It is given the field
  // names in a list so that databases that are capable of deleting fields atomically
  // may do so.

  // Returns a Promise.
  async deleteFields(className: string, schema: SchemaType, fieldNames: string[]): Promise<void> {
    debug('deleteFields', className, fieldNames);

    const columnsToDelete = fieldNames.reduce((list, fieldName) => {
      const field = schema.fields[fieldName];
      if (field.type !== 'Relation') {
        list.push(fieldName);
      }
      delete schema.fields[fieldName];
      return list;
    }, []);

    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      await connection.execute(
        `UPDATE "_SCHEMA" SET "schema" = :schema WHERE "className" = :className`,
        {
          schema: JSON.stringify(schema),
          className: className,
        }
      );

      if (columnsToDelete.length > 0) {
        for (const fieldName of columnsToDelete) {
          try {
            await connection.execute(`ALTER TABLE "${className}" DROP COLUMN "${fieldName}"`);
          } catch (error) {
            // ORA-00904: invalid identifier (column does not exist)
            if (error.errorNum !== 904) {
              throw error;
            }
          }
        }
      }

      await connection.commit();
      this._notifySchemaChange();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      await connection.close();
    }
  }

  // Return a promise for all schemas known to this adapter, in Parse format. In case the
  // schemas cannot be retrieved, returns a promise that rejects. Requirements for the
  // rejection reason are TBD.
  async getAllClasses() {
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      const result = await connection.execute(
        'SELECT * FROM "_SCHEMA"',
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      return result.rows.map(row =>
        toParseSchema({
          className: row.className,
          ...row.schema,
        })
      );
    } catch (error) {
      if (error.errorNum === 942) {
        return [];
      }
      throw error;
    } finally {
      await connection.close();
    }
  }

  // Return a promise for the schema with the given name, in Parse format. If
  // this adapter doesn't know about the schema, return a promise that rejects with
  // undefined as the reason.
  async getClass(className: string) {
    debug('getClass');
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    return connection
      .execute(`SELECT * FROM "_SCHEMA" WHERE "className" = :className`, {
        className: className,
      })
      .then(result => {
        if (result.length !== 1) {
          throw undefined;
        }
        return result[0].schema;
      })
      .then(toParseSchema)
      .then(() => connection.close());
  }

  // TODO: remove the mongo format dependency in the return value
  async createObject(
    className: string,
    schema: SchemaType,
    object: any,
    transactionalSession?: any
  ) {
    debug('createObject', className);

    const columnsArray = [];
    const valuesArray = [];
    const dateFields = new Set(); // Track which fields are Date fields (for special _User fields not in schema)
    schema = toOracleSchema(schema);
    const geoPoints = {};

    object = handleDotFields(object);
    validateKeys(object);

    Object.keys(object).forEach(fieldName => {
      if (object[fieldName] === null) {
        return;
      }

      const authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
      const authDataAlreadyExists = !!object.authData;
      if (authDataMatch) {
        const provider = authDataMatch[1];
        object['authData'] = object['authData'] || {};
        object['authData'][provider] = object[fieldName];
        delete object[fieldName];
        fieldName = 'authData';
        if (authDataAlreadyExists) {
          return;
        }
      }

      if (!schema.fields[fieldName] && className === '_User') {
        let valueAdded = false;
        
        if (
          fieldName === '_email_verify_token' ||
          fieldName === '_failed_login_count' ||
          fieldName === '_perishable_token' ||
          fieldName === '_password_history'
        ) {
          valueAdded = true;
        }

        if (fieldName === '_email_verify_token_expires_at') {
          valueAdded = true;
        }

        if (
          fieldName === '_account_lockout_expires_at' ||
          fieldName === '_perishable_token_expires_at' ||
          fieldName === '_password_changed_at'
        ) {
          valueAdded = true;
        }
        
        if (valueAdded) {
          columnsArray.push(fieldName);
          if (
            fieldName === '_email_verify_token' ||
            fieldName === '_failed_login_count' ||
            fieldName === '_perishable_token' ||
            fieldName === '_password_history'
          ) {
            valuesArray.push(object[fieldName]);
          } else if (fieldName === '_email_verify_token_expires_at') {
            dateFields.add(fieldName);
            valuesArray.push(object[fieldName] ? object[fieldName].iso : null);
          } else if (
            fieldName === '_account_lockout_expires_at' ||
            fieldName === '_perishable_token_expires_at' ||
            fieldName === '_password_changed_at'
          ) {
            dateFields.add(fieldName);
            valuesArray.push(object[fieldName] ? object[fieldName].iso : null);
          }
          return;
        }
        // If field doesn't match any conditions, skip it (don't add to columnsArray)
        return;
      }

      // Ensure field exists in schema before processing
      if (!schema.fields[fieldName]) {
        // Skip fields not in schema (they should have been handled earlier)
        return;
      }

      columnsArray.push(fieldName);

      switch (schema.fields[fieldName].type) {
        case 'Date':
          valuesArray.push(object[fieldName] ? object[fieldName].iso : null);
          break;
        case 'Pointer':
          valuesArray.push(object[fieldName].objectId);
          break;
        case 'Array':
          valuesArray.push(JSON.stringify(object[fieldName]));
          break;
        case 'Object':
          // Stringify Object types for Oracle JSON columns
          valuesArray.push(JSON.stringify(object[fieldName]));
          break;
        case 'Bytes':
        case 'String':
        case 'Number':
        case 'Boolean':
          valuesArray.push(object[fieldName]);
          break;
        case 'File':
          valuesArray.push(object[fieldName].name);
          break;
        case 'Polygon': {
          const value = convertPolygonToSQL(object[fieldName].coordinates);
          valuesArray.push(value);
          break;
        }
        case 'GeoPoint':
          geoPoints[fieldName] = object[fieldName];
          columnsArray.pop();
          break;
        default:
          throw new Error(`Type ${schema.fields[fieldName].type} not supported yet`);
      }
    });

    // Validate that columnsArray and valuesArray have the same length
    if (columnsArray.length !== valuesArray.length) {
      const errorMsg = `Column/value mismatch in createObject: columnsArray.length=${columnsArray.length}, valuesArray.length=${valuesArray.length}. Columns: [${columnsArray.join(', ')}], className: ${className}`;
      debug(errorMsg);
      throw new Parse.Error(
        Parse.Error.INTERNAL_SERVER_ERROR,
        errorMsg
      );
    }

    const allColumns = [...columnsArray, ...Object.keys(geoPoints)];
    const binds = {};
    const columnToBindIndex = new Map(); // Map column index to sequential bind index
    let bindIndex = 0; // Sequential index for bind parameters

    // First pass: create bind parameters and map column indices to bind indices
    columnsArray.forEach((col, colIndex) => {
      const value = valuesArray[colIndex];
      const isDateField = (schema.fields[col] && schema.fields[col].type === 'Date') || dateFields.has(col);
      
      // Check if field is in schema or is a special _User field
      if (!schema.fields[col] && !dateFields.has(col) && className !== '_User') {
        const errorMsg = `Field "${col}" not found in schema for class "${className}". Available fields: [${Object.keys(schema.fields).join(', ')}]`;
        debug(errorMsg);
        throw new Parse.Error(
          Parse.Error.INTERNAL_SERVER_ERROR,
          errorMsg
        );
      }
      
      // For null Date fields, use NULL directly in SQL (no bind parameter)
      if (isDateField && (value === null || value === undefined)) {
        columnToBindIndex.set(colIndex, null); // Mark as null (no bind)
        return;
      }
      
      // Create bind parameter with sequential index
      const bindName = `val${bindIndex}`;
      columnToBindIndex.set(colIndex, bindIndex);
      
      if (schema.fields[col] && ['Array', 'Bytes', 'Object'].includes(schema.fields[col].type)) {
        binds[bindName] = JSON.stringify(value);
      } else {
        binds[bindName] = value;
      }
      
      bindIndex++;
    });

    Object.keys(geoPoints).forEach((key, index) => {
      const value = geoPoints[key];
      binds[`geo${index}_lon`] = value.longitude;
      binds[`geo${index}_lat`] = value.latitude;
    });

    // Validate we have at least one column to insert
    if (allColumns.length === 0) {
      const errorMsg = `No columns to insert for class "${className}". Object keys: [${Object.keys(object).join(', ')}]`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }

    const columnsList = allColumns.map(col => `"${col}"`).join(', ');

    const valuesList = [];
    for (let colIndex = 0; colIndex < columnsArray.length; colIndex++) {
      const col = columnsArray[colIndex];
      // Check if field is a Date field (either in schema or in dateFields set)
      const isDateField = (schema.fields[col] && schema.fields[col].type === 'Date') || dateFields.has(col);
      const value = valuesArray[colIndex];
      const bindIdx = columnToBindIndex.get(colIndex);
      
      // For Date fields with null values, use NULL directly instead of TO_TIMESTAMP
      if (isDateField && (bindIdx === null || bindIdx === undefined || value === null || value === undefined)) {
        valuesList.push('NULL');
        continue;
      }
      
      // Validate bind index exists
      if (bindIdx === null || bindIdx === undefined) {
        const errorMsg = `No bind index found for column "${col}" at index ${colIndex}. columnsArray.length=${columnsArray.length}, valuesArray.length=${valuesArray.length}, columnToBindIndex entries: ${Array.from(columnToBindIndex.entries()).map(([k, v]) => `${k}->${v}`).join(', ')}`;
        console.error(errorMsg);
        throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
      }
      
      // Validate bind parameter exists
      const bindName = `val${bindIdx}`;
      if (!binds.hasOwnProperty(bindName)) {
        const errorMsg = `Bind parameter "${bindName}" not found for column "${col}" at index ${colIndex}. Available binds: ${Object.keys(binds).sort().join(', ')}`;
        console.error(errorMsg);
        throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
      }
      
      // Use the sequential bind index
      if (isDateField) {
        valuesList.push(`TO_TIMESTAMP(:${bindName}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')`);
      } else {
        valuesList.push(`:${bindName}`);
      }
    }
    
    // Add geoPoints values
    Object.keys(geoPoints).forEach((key, i) => {
      valuesList.push(`SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:geo${i}_lon, :geo${i}_lat, NULL), NULL, NULL)`);
    });

    if (valuesList.length === 0) {
      const errorMsg = `No values to insert for class "${className}". columnsArray.length=${columnsArray.length}, valuesArray.length=${valuesArray.length}`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }

    // Validate valuesList doesn't contain undefined or null entries (should all be strings)
    const invalidValues = valuesList.filter(v => v === undefined || v === null || typeof v !== 'string');
    if (invalidValues.length > 0) {
      const errorMsg = `Invalid values in valuesList for class "${className}": ${invalidValues.length} invalid entries. valuesList: [${valuesList.map((v, i) => `${i}=${typeof v}=${v}`).join(', ')}]`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    const valuesListStr = valuesList.join(', ');
    
    // Validate columnsList and valuesListStr are not empty
    if (!columnsList || columnsList.trim() === '') {
      const errorMsg = `Empty columnsList for class "${className}". allColumns: [${allColumns.join(', ')}]`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    if (!valuesListStr || valuesListStr.trim() === '') {
      const errorMsg = `Empty valuesListStr for class "${className}". valuesList: [${valuesList.join(', ')}]`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    // Check for trailing/leading commas or double commas which would cause syntax errors
    if (columnsList.startsWith(',') || columnsList.endsWith(',') || columnsList.includes(',,')) {
      const errorMsg = `Malformed columnsList for class "${className}": "${columnsList}"`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    if (valuesListStr.startsWith(',') || valuesListStr.endsWith(',') || valuesListStr.includes(',,')) {
      const errorMsg = `Malformed valuesListStr for class "${className}": "${valuesListStr}"`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    // Critical validation: ensure columns and values match
    const columnCount = allColumns.length;
    const valueCount = valuesList.length;
    if (columnCount !== valueCount) {
      const errorMsg = `Column/value count mismatch in createObject for class "${className}": ${columnCount} columns but ${valueCount} values. columnsArray.length=${columnsArray.length}, geoPoints count=${Object.keys(geoPoints).length}, valuesList.length=${valuesList.length}`;
      console.error(errorMsg);
      console.error(`columnsList: "${columnsList}"`);
      console.error(`valuesListStr: "${valuesListStr}"`);
      console.error(`allColumns:`, allColumns);
      console.error(`valuesList:`, valuesList);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    const insertSql = `INSERT INTO "${className}" (${columnsList}) VALUES (${valuesListStr})`;
    
    // Validate SQL syntax - check for balanced parentheses
    // Count parentheses, but ignore those inside string literals
    let openParens = 0;
    let closeParens = 0;
    let inString = false;
    let stringChar = null;
    for (let i = 0; i < insertSql.length; i++) {
      const char = insertSql[i];
      const prevChar = i > 0 ? insertSql[i - 1] : null;
      
      // Check if we're entering or leaving a string literal
      if ((char === "'" || char === '"') && prevChar !== '\\') {
        if (!inString) {
          inString = true;
          stringChar = char;
        } else if (char === stringChar) {
          inString = false;
          stringChar = null;
        }
        continue;
      }
      
      // Only count parentheses outside of string literals
      if (!inString) {
        if (char === '(') openParens++;
        if (char === ')') closeParens++;
      }
    }
    
    if (openParens !== closeParens) {
      const errorMsg = `Unbalanced parentheses in SQL for class "${className}": ${openParens} open, ${closeParens} close. SQL: ${insertSql}`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }
    
    // Additional validation: check for common SQL syntax issues
    if (insertSql.includes('()') || insertSql.includes('( )')) {
      const errorMsg = `Empty parentheses detected in SQL for class "${className}". SQL: ${insertSql}`;
      console.error(errorMsg);
      throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, errorMsg);
    }

    // Always log SQL for _User class to help debug
    if (className === '_User') {
      console.log(`[DEBUG] createObject SQL for _User:`);
      console.log(`  SQL: ${insertSql}`);
      console.log(`  Columns (${columnCount}):`, allColumns);
      console.log(`  Values (${valueCount}):`, valuesList);
      console.log(`  Binds:`, Object.keys(binds).sort().map(k => `${k}=${typeof binds[k]}`));
    }

    // Debug logging to help diagnose issues - always log for _User class
    if (columnsArray.length !== valuesArray.length || className === '_User') {
      debug(`createObject SQL generation for class "${className}":`);
      debug(`  columnsArray.length=${columnsArray.length}, valuesArray.length=${valuesArray.length}`);
      debug(`  columnsArray:`, columnsArray);
      debug(`  valuesArray types:`, valuesArray.map(v => typeof v));
      debug(`  dateFields:`, Array.from(dateFields));
      debug(`  columnToBindIndex:`, Array.from(columnToBindIndex.entries()));
      debug(`  columnsList:`, columnsList);
      debug(`  valuesList:`, valuesList);
      debug(`  insertSql:`, insertSql);
      debug(`  binds keys:`, Object.keys(binds));
      debug(`  binds values:`, Object.values(binds).map(v => typeof v === 'string' ? v.substring(0, 50) : v));
    }

    const pool = await this._pgp;
    const connection = transactionalSession || (await pool.getConnection());
    const shouldCloseConnection = !transactionalSession;

    try {
      // Log the exact SQL and binds before execution for debugging
      if (className === '_User') {
        console.error(`[ERROR DEBUG] About to execute SQL for _User:`);
        console.error(`  SQL: ${insertSql}`);
        console.error(`  Binds count: ${Object.keys(binds).length}`);
        console.error(`  Binds:`, JSON.stringify(Object.keys(binds).reduce((acc, k) => {
          const val = binds[k];
          acc[k] = typeof val === 'string' ? (val.length > 100 ? val.substring(0, 100) + '...' : val) : val;
          return acc;
        }, {}), null, 2));
      }
      
      await connection.execute(insertSql, binds);

      if (shouldCloseConnection) {
        await connection.commit();
      }

      return { ops: [object] };
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }

      // Enhanced error logging for SQL syntax errors
      if (error.errorNum === 907 || (error.message && error.message.includes('ORA-00907'))) {
        const errorDetails = {
          className,
          sql: insertSql,
          columnsArray: columnsArray,
          valuesArray: valuesArray.map(v => typeof v === 'object' ? JSON.stringify(v).substring(0, 100) : v),
          bindsKeys: Object.keys(binds).sort(),
          bindsValues: Object.keys(binds).sort().reduce((acc, k) => {
            const val = binds[k];
            acc[k] = typeof val === 'string' ? (val.length > 50 ? val.substring(0, 50) + '...' : val) : val;
            return acc;
          }, {}),
          dateFields: Array.from(dateFields),
          columnToBindIndex: Array.from(columnToBindIndex.entries()),
          columnsList,
          valuesList,
          allColumns,
          objectKeys: Object.keys(object),
          openParens,
          closeParens
        };
        console.error(`[CRITICAL] SQL syntax error (ORA-00907) in createObject:`, JSON.stringify(errorDetails, null, 2));
        console.error(`SQL: ${insertSql}`);
        console.error(`Columns: [${allColumns.join(', ')}]`);
        console.error(`Values: [${valuesList.join(', ')}]`);
        console.error(`Binds:`, binds);
      }

      // ORA-00001: unique constraint violated
      if (error.errorNum === 1) {
        const err = new Parse.Error(
          Parse.Error.DUPLICATE_VALUE,
          'A duplicate value for a field with unique values was provided'
        );
        err.underlyingError = error;

        const constraintMatch = error.message.match(/\(([^.]+)\.([^)]+)\)/);
        if (constraintMatch) {
          const constraintName = constraintMatch[2];
          const fieldMatch = constraintName.match(/unique_([a-zA-Z]+)/);
          if (fieldMatch) {
            err.userInfo = { duplicated_field: fieldMatch[1] };
          }
        }
        throw err;
      }

      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  // Remove all objects that match the given Parse Query.
  // If no objects match, reject with OBJECT_NOT_FOUND. If objects are found and deleted, resolve with undefined.
  // If there is some other error, reject with INTERNAL_SERVER_ERROR.
  async deleteObjectsByQuery(
    className: string,
    schema: SchemaType,
    query: QueryType,
    transactionalSession?: any
  ) {
    debug('deleteObjectsByQuery', className);

    const where = buildWhereClause({ schema, query, caseInsensitive: false });
    const wherePattern = Object.keys(query).length === 0 ? '1=1' : where.pattern;

    await this._pgp;
    const pool = await this._pgp;
    const connection = transactionalSession || (await pool.getConnection());
    const shouldCloseConnection = !transactionalSession;

    try {
      const selectSql = `SELECT * FROM "${className}" WHERE ${wherePattern}`;
      console.error(`[SQL DEBUG] About to execute DELETE query for class "${className}":`);
      console.error(`  SELECT SQL: ${selectSql}`);
      console.error(`  DELETE SQL: DELETE FROM "${className}" WHERE ${wherePattern}`);
      console.error(`  Binds:`, JSON.stringify(where.binds || {}, null, 2));
      
      const selectResult = await connection.execute(selectSql, where.binds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      if (selectResult.rows.length === 0) {
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
      }

      const deleteSql = `DELETE FROM "${className}" WHERE ${wherePattern}`;
      await connection.execute(deleteSql, where.binds);

      if (shouldCloseConnection) {
        await connection.commit();
      }

      return {
        count: selectResult.rows.length,
        objects: selectResult.rows,
      };
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }

      if (error.errorNum === 942) {
        return { count: 0, objects: [] };
      }
      
      // Enhanced error logging for SQL syntax errors
      if (error.errorNum === 907 || (error.message && error.message.includes('ORA-00907'))) {
        console.error(`[CRITICAL] SQL syntax error (ORA-00907) in deleteObjectsByQuery for class "${className}":`);
        console.error(`  SELECT SQL: SELECT * FROM "${className}" WHERE ${wherePattern}`);
        console.error(`  DELETE SQL: DELETE FROM "${className}" WHERE ${wherePattern}`);
        console.error(`  Binds:`, JSON.stringify(where.binds || {}, null, 2));
      }

      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }
  // Return value not currently well specified.
  async findOneAndUpdate(
    className: string,
    schema: SchemaType,
    query: QueryType,
    update: any,
    transactionalSession: ?any
  ): Promise<any> {
    debug('findOneAndUpdate');
    return this.updateObjectsByQuery(className, schema, query, update, transactionalSession).then(
      val => {
        if (!val || val.length === 0 || !val[0]) {
          throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
        }
        return val[0];
      }
    );
  }

  // Apply the update to all objects that match the given Parse Query.
  async updateObjectsByQuery(
    className: string,
    schema: SchemaType,
    query: QueryType,
    update: any,
    transactionalSession?: any
  ): Promise<any[]> {
    debug('updateObjectsByQuery', className);

    schema = toOracleSchema(schema);
    const originalUpdate = { ...update };
    const binds = {};
    let bindIndex = 0;

    const getBindName = () => `val${bindIndex++}`;

    const dotNotationOptions = {};
    Object.keys(update).forEach(fieldName => {
      dotNotationOptions[fieldName] = fieldName.indexOf('.') > -1;
    });

    update = handleDotFields(update);

    for (const fieldName in update) {
      const authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
      if (authDataMatch) {
        const provider = authDataMatch[1];
        const value = update[fieldName];
        delete update[fieldName];
        update['authData'] = update['authData'] || {};
        update['authData'][provider] = value;
      }
    }

    const updatePatterns = [];

    for (const fieldName in update) {
      const fieldValue = update[fieldName];

      if (typeof fieldValue === 'undefined') {
        delete update[fieldName];
        continue;
      }

      if (fieldValue === null) {
        updatePatterns.push(`"${fieldName}" = NULL`);
      } else if (fieldName === 'authData') {
        const authUpdates = [];
        for (const key in fieldValue) {
          const bindName = getBindName();
          const value = fieldValue[key];
          if (value && value.__op === 'Delete') {
            binds[bindName] = null;
          } else {
            binds[bindName] = JSON.stringify(value);
          }
          authUpdates.push(
            `'${key}', ${value && value.__op === 'Delete' ? 'NULL' : `:${bindName}`}`
          );
        }
        updatePatterns.push(
          `"${fieldName}" = JSON_MERGEPATCH(COALESCE("${fieldName}", '{}'), JSON_OBJECT(${authUpdates.join(
            ', '
          )}))`
        );
      } else if (fieldValue.__op === 'Increment') {
        const bindName = getBindName();
        binds[bindName] = fieldValue.amount;
        updatePatterns.push(`"${fieldName}" = COALESCE("${fieldName}", 0) + :${bindName}`);
      } else if (fieldValue.__op === 'Add') {
        const bindName = getBindName();
        binds[bindName] = JSON.stringify(fieldValue.objects);
        updatePatterns.push(
          `"${fieldName}" = array_add(COALESCE("${fieldName}", '[]'), :${bindName})`
        );
      } else if (fieldValue.__op === 'Delete') {
        updatePatterns.push(`"${fieldName}" = NULL`);
      } else if (fieldValue.__op === 'Remove') {
        const bindName = getBindName();
        binds[bindName] = JSON.stringify(fieldValue.objects);
        updatePatterns.push(
          `"${fieldName}" = array_remove(COALESCE("${fieldName}", '[]'), :${bindName})`
        );
      } else if (fieldValue.__op === 'AddUnique') {
        const bindName = getBindName();
        binds[bindName] = JSON.stringify(fieldValue.objects);
        updatePatterns.push(
          `"${fieldName}" = array_add_unique(COALESCE("${fieldName}", '[]'), :${bindName})`
        );
      } else if (fieldValue.__type === 'Pointer') {
        const bindName = getBindName();
        binds[bindName] = fieldValue.objectId;
        updatePatterns.push(`"${fieldName}" = :${bindName}`);
      } else if (fieldValue.__type === 'Date') {
        const bindName = getBindName();
        binds[bindName] = toOracleValue(fieldValue);
        updatePatterns.push(
          `"${fieldName}" = TO_TIMESTAMP(:${bindName}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')`
        );
      } else if (fieldValue instanceof Date) {
        const bindName = getBindName();
        binds[bindName] = fieldValue.toISOString();
        updatePatterns.push(
          `"${fieldName}" = TO_TIMESTAMP(:${bindName}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')`
        );
      } else if (fieldValue.__type === 'File') {
        const bindName = getBindName();
        binds[bindName] = toOracleValue(fieldValue);
        updatePatterns.push(`"${fieldName}" = :${bindName}`);
      } else if (fieldValue.__type === 'GeoPoint') {
        const lonBind = getBindName();
        const latBind = getBindName();
        binds[lonBind] = fieldValue.longitude;
        binds[latBind] = fieldValue.latitude;
        updatePatterns.push(
          `"${fieldName}" = SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(:${lonBind}, :${latBind}, NULL), NULL, NULL)`
        );
      } else if (fieldValue.__type === 'Polygon') {
        const bindName = getBindName();
        binds[bindName] = convertPolygonToSQL(fieldValue.coordinates);
        updatePatterns.push(`"${fieldName}" = :${bindName}`);
      } else if (fieldValue.__type === 'Relation') {
        // noop
      } else if (
        typeof fieldValue === 'object' &&
        schema.fields[fieldName] &&
        schema.fields[fieldName].type === 'Object'
      ) {
        const keysToIncrement = Object.keys(originalUpdate)
          .filter(k => {
            const value = originalUpdate[k];
            return (
              value &&
              value.__op === 'Increment' &&
              k.split('.').length === 2 &&
              k.split('.')[0] === fieldName
            );
          })
          .map(k => k.split('.')[1]);

        const keysToDelete = Object.keys(originalUpdate)
          .filter(k => {
            const value = originalUpdate[k];
            return (
              value &&
              value.__op === 'Delete' &&
              k.split('.').length === 2 &&
              k.split('.')[0] === fieldName
            );
          })
          .map(k => k.split('.')[1]);

        keysToIncrement.forEach(key => delete fieldValue[key]);

        const bindName = getBindName();
        binds[bindName] = JSON.stringify(fieldValue);

        let updateExpr = dotNotationOptions[fieldName] ? `COALESCE("${fieldName}", '{}')` : `'{}'`;

        keysToDelete.forEach(key => {
          updateExpr = `JSON_REMOVE(${updateExpr}, '$.${key}')`;
        });

        keysToIncrement.forEach(key => {
          const amount = originalUpdate[`${fieldName}.${key}`].amount;
          updateExpr = `JSON_SET(${updateExpr}, '$.${key}', JSON_EXTRACT(${updateExpr}, '$.${key}') + ${amount})`;
        });

        updatePatterns.push(`"${fieldName}" = JSON_MERGEPATCH(${updateExpr}, :${bindName})`);
      } else if (
        Array.isArray(fieldValue) &&
        schema.fields[fieldName] &&
        schema.fields[fieldName].type === 'Array'
      ) {
        const bindName = getBindName();
        binds[bindName] = JSON.stringify(fieldValue);
        updatePatterns.push(`"${fieldName}" = :${bindName}`);
      } else if (
        typeof fieldValue === 'string' ||
        typeof fieldValue === 'number' ||
        typeof fieldValue === 'boolean'
      ) {
        const bindName = getBindName();
        // Check if this is a date field (updatedAt, createdAt) or if the schema indicates it's a Date type
        const isDateField = (fieldName === 'updatedAt' || fieldName === 'createdAt') ||
                            (schema.fields[fieldName] && schema.fields[fieldName].type === 'Date');
        // Check if the string looks like an ISO 8601 date string
        const isISODateString = typeof fieldValue === 'string' && 
                                /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z?$/.test(fieldValue);
        
        if (isDateField && isISODateString) {
          binds[bindName] = fieldValue;
          updatePatterns.push(
            `"${fieldName}" = TO_TIMESTAMP(:${bindName}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')`
          );
        } else {
          binds[bindName] = fieldValue;
          updatePatterns.push(`"${fieldName}" = :${bindName}`);
        }
      } else {
        debug('Not supported update', { fieldName, fieldValue });
        throw new Parse.Error(
          Parse.Error.OPERATION_FORBIDDEN,
          `Oracle doesn't support update ${JSON.stringify(fieldValue)} yet`
        );
      }
    }

    // Ensure we're passing the correct bindIndex
    if (bindIndex === undefined || bindIndex === null) {
      throw new Parse.Error(
        Parse.Error.INTERNAL_SERVER_ERROR,
        `Invalid bindIndex: ${bindIndex}`
      );
    }
    
    
    const where = buildWhereClause({
      schema,
      query,
      caseInsensitive: false,
      startBindIndex: bindIndex,
      className: className,
    });

    // Debug: Log before merging - use console.error to ensure it's visible
    const debugMsg = `Before merging WHERE binds - UPDATE bindIndex: ${bindIndex}, UPDATE binds: [${Object.keys(binds).join(', ')}], WHERE binds: [${Object.keys(where.binds).join(', ')}], WHERE pattern: ${where.pattern}`;
    debug(debugMsg);
    
    // Verify that WHERE clause bind names don't conflict with UPDATE bind names
    const conflictingBinds = Object.keys(where.binds).filter(key => binds[key] !== undefined);
    if (conflictingBinds.length > 0) {
      const errorMsg = `Bind name conflict detected: ${conflictingBinds.join(', ')}. UPDATE binds: [${Object.keys(binds).join(', ')}], WHERE binds: [${Object.keys(where.binds).join(', ')}], bindIndex: ${bindIndex}`;
      console.error('OracleStorageAdapter ERROR:', errorMsg);
      throw new Parse.Error(
        Parse.Error.INTERNAL_SERVER_ERROR,
        errorMsg
      );
    }

    // Merge WHERE clause binds - ensure no conflicts
    for (const key in where.binds) {
      if (binds[key] !== undefined) {
        // This should not happen if bind indices are managed correctly
        debug(`Warning: bind name conflict detected: ${key}. UPDATE binds:`, Object.keys(binds), 'WHERE binds:', Object.keys(where.binds));
        throw new Parse.Error(
          Parse.Error.INTERNAL_SERVER_ERROR,
          `Bind name conflict: ${key} already exists in UPDATE binds`
        );
      }
      binds[key] = where.binds[key];
    }
    
    debug('After merging - All binds:', Object.keys(binds), 'bindIndex:', bindIndex, 'WHERE pattern:', where.pattern);

    const whereClause = where.pattern.length > 0 ? `WHERE ${where.pattern}` : '';

    // Validate that we have a WHERE clause - updating without a WHERE clause would update all rows
    if (whereClause === '') {
      throw new Parse.Error(
        Parse.Error.INVALID_QUERY,
        'Cannot update without a WHERE clause'
      );
    }

    await this._pgp;
    const pool = await this._pgp;
    const connection = transactionalSession || (await pool.getConnection());
    const shouldCloseConnection = !transactionalSession;

    try {
      const selectSql = `SELECT * FROM "${className}" ${whereClause}`;
      
      console.error(`[SQL DEBUG] About to execute UPDATE query for class "${className}":`);
      console.error(`  SELECT SQL: ${selectSql}`);
      console.error(`  Binds:`, JSON.stringify(binds || {}, null, 2));
      
      // Extract only the bind variables actually used in the SELECT SQL
      // Oracle may be strict about only passing bind variables that are referenced in the SQL
      // Extract bind variables while ignoring those inside string literals
      const extractBindVariables = (sql) => {
        const bindVars = [];
        let inString = false;
        let stringChar = null;
        for (let i = 0; i < sql.length; i++) {
          const char = sql[i];
          const prevChar = i > 0 ? sql[i - 1] : null;
          
          // Check if we're entering or leaving a string literal
          if ((char === "'" || char === '"') && prevChar !== '\\') {
            if (!inString) {
              inString = true;
              stringChar = char;
            } else if (char === stringChar) {
              inString = false;
              stringChar = null;
            }
            continue;
          }
          
          // Only look for bind variables outside of string literals
          if (!inString && char === ':') {
            let bindName = '';
            let j = i + 1;
            while (j < sql.length && /[a-zA-Z0-9_]/.test(sql[j])) {
              bindName += sql[j];
              j++;
            }
            if (bindName) {
              bindVars.push(bindName);
            }
          }
        }
        return [...new Set(bindVars)]; // Deduplicate
      };
      
      const selectBindNames = extractBindVariables(selectSql);
      const selectBinds = {};
      const missingBinds = [];
      for (const bindName of selectBindNames) {
        if (binds[bindName] === undefined) {
          missingBinds.push(bindName);
        } else {
          selectBinds[bindName] = binds[bindName];
        }
      }
      if (missingBinds.length > 0) {
        const errorMsg = `Missing bind variable(s) in SELECT: ${missingBinds.join(', ')}. Available binds: ${Object.keys(binds).join(', ')}. WHERE clause: ${whereClause}. SELECT SQL: ${selectSql}. UPDATE patterns: ${updatePatterns.join(', ')}`;
        console.error('OracleStorageAdapter ERROR:', errorMsg);
        debug(errorMsg);
        throw new Parse.Error(
          Parse.Error.INTERNAL_SERVER_ERROR,
          errorMsg
        );
      }
      
      
      // First, verify the object exists before attempting update
      let preCheckResult;
      try {
        preCheckResult = await connection.execute(selectSql, selectBinds, {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
        });
      } catch (selectError) {
        // ORA-06575: Package or function is in an invalid state
        // Try to recompile the array_contains function and retry
        const isInvalidStateError = selectError.errorNum === 6575 || 
                                   (selectError.message && selectError.message.includes('ORA-06575')) ||
                                   (selectError.code === 'ORA-06575');
        if (isInvalidStateError && selectSql.includes('array_contains')) {
          debug('array_contains function is invalid, attempting to drop and recreate...');
          try {
            // Try to drop the function first, ignore if it doesn't exist
            try {
              await connection.execute('DROP FUNCTION array_contains');
            } catch (dropError) {
              // Ignore errors if function doesn't exist (ORA-04043: object does not exist)
              if (dropError.errorNum !== 4043) {
                debug('Drop function error (may be ignored):', dropError.message);
              }
            }
            // Now create it fresh
            await connection.execute(sql.array.contains);
            await connection.commit();
            debug('array_contains function recreated successfully, retrying query...');
            // Retry the query after recreating
            preCheckResult = await connection.execute(selectSql, selectBinds, {
              outFormat: oracledb.OUT_FORMAT_OBJECT,
            });
          } catch (recompileError) {
            debug('Failed to recreate array_contains function:', recompileError.message);
            throw selectError; // Throw the original error
          }
        } else {
          throw selectError;
        }
      }
      
      if (preCheckResult.rows.length === 0) {
        // Object doesn't exist - try to find it without _wperm condition to debug
        const objectIdValue = query.objectId || (query.$and && query.$and[0] && query.$and[0].objectId);
        if (objectIdValue) {
          const debugSql = `SELECT * FROM "${className}" WHERE "objectId" = :objId`;
          const debugResult = await connection.execute(debugSql, { objId: objectIdValue }, {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
          });
          
          // Test JSON_TABLE query to see if it works
          if (debugResult.rows.length > 0 && debugResult.rows[0]._wperm) {
            const testValue = objectIdValue; // Use the actual objectId
            const testSql = `SELECT COUNT(*) as cnt FROM JSON_TABLE(:wperm, '$[*]' COLUMNS (value VARCHAR2(4000) PATH '$')) WHERE CAST(value AS VARCHAR2(4000)) = CAST(:testVal AS VARCHAR2(4000))`;
            try {
              const testResult = await connection.execute(testSql, { 
                wperm: debugResult.rows[0]._wperm, 
                testVal: testValue 
              }, {
                outFormat: oracledb.OUT_FORMAT_OBJECT,
              });
              
              // Test JSON_EXISTS directly
              const testJsonExistsSql = `SELECT COUNT(*) as cnt FROM "_User" WHERE "objectId" = :objId AND JSON_EXISTS("_wperm", '$[*]?(@ == :testVal)')`;
              try {
                const testJsonExistsResult = await connection.execute(testJsonExistsSql, { 
                  objId: objectIdValue,
                  testVal: testValue 
                }, {
                  outFormat: oracledb.OUT_FORMAT_OBJECT,
                });
              } catch (jsonExistsError) {
                // Ignore test errors
              }
              
              // Test scalar subquery with COUNT (same as what we use in WHERE clause)
              const testScalarSubquerySql = `SELECT COUNT(*) as cnt FROM "_User" WHERE "objectId" = :objId AND (SELECT COUNT(*) FROM JSON_TABLE("_wperm", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')) jt WHERE jt.val = :testVal) > 0`;
              try {
                const testScalarResult = await connection.execute(testScalarSubquerySql, { 
                  objId: objectIdValue,
                  testVal: testValue 
                }, {
                  outFormat: oracledb.OUT_FORMAT_OBJECT,
                });
              } catch (scalarError) {
                // Ignore test errors
              }
              
              // Test using table alias in the subquery
              const testTableAliasSql = `SELECT COUNT(*) as cnt FROM "_User" u WHERE u."objectId" = :objId AND (SELECT COUNT(*) FROM JSON_TABLE(u."_wperm", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')) jt WHERE jt.val = :testVal) > 0`;
              try {
                const testTableAliasResult = await connection.execute(testTableAliasSql, { 
                  objId: objectIdValue,
                  testVal: testValue 
                }, {
                  outFormat: oracledb.OUT_FORMAT_OBJECT,
                });
              } catch (tableAliasError) {
                // Ignore test errors
              }
            } catch (testError) {
              // Ignore test errors
            }
          }
        }
        
        // Object doesn't exist
        if (shouldCloseConnection) {
          await connection.rollback();
        }
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
      }

      if (updatePatterns.length > 0) {
        const updateSql = `UPDATE "${className}" SET ${updatePatterns.join(', ')} ${whereClause}`;
        
        console.error(`[SQL DEBUG] About to execute UPDATE statement for class "${className}":`);
        console.error(`  UPDATE SQL: ${updateSql}`);
        console.error(`  Binds:`, JSON.stringify(binds || {}, null, 2));
        
        // Helper function to recompile array_contains if needed
        const recompileArrayContainsIfNeeded = async (error, sqlText) => {
          const isInvalidStateError = error.errorNum === 6575 || 
                                     (error.message && error.message.includes('ORA-06575')) ||
                                     (error.code === 'ORA-06575') ||
                                     (error.message && error.message.includes('ARRAY_CONTAINS') && error.message.includes('invalid state'));
          if (isInvalidStateError && sqlText.includes('array_contains')) {
            debug('array_contains function is invalid, attempting to drop and recreate...');
            try {
              // Try to drop the function first, ignore if it doesn't exist
              try {
                await connection.execute('DROP FUNCTION array_contains');
              } catch (dropError) {
                // Ignore errors if function doesn't exist
                debug('Drop function error (ignored):', dropError.message);
              }
              // Now create it fresh
              await connection.execute(sql.array.contains);
              await connection.commit();
              debug('array_contains function recreated successfully');
              return true;
            } catch (recompileError) {
              debug('Failed to recreate array_contains function:', recompileError.message);
              return false;
            }
          }
          return false;
        };
        
        // Verify all bind variables in the UPDATE SQL exist in binds
        // Extract bind variables while ignoring those inside string literals
        const extractBindVariables = (sql) => {
          const bindVars = [];
          let inString = false;
          let stringChar = null;
          for (let i = 0; i < sql.length; i++) {
            const char = sql[i];
            const prevChar = i > 0 ? sql[i - 1] : null;
            
            // Check if we're entering or leaving a string literal
            if ((char === "'" || char === '"') && prevChar !== '\\') {
              if (!inString) {
                inString = true;
                stringChar = char;
              } else if (char === stringChar) {
                inString = false;
                stringChar = null;
              }
              continue;
            }
            
            // Only look for bind variables outside of string literals
            if (!inString && char === ':') {
              let bindName = '';
              let j = i + 1;
              while (j < sql.length && /[a-zA-Z0-9_]/.test(sql[j])) {
                bindName += sql[j];
                j++;
              }
              if (bindName) {
                bindVars.push(bindName);
              }
            }
          }
          return [...new Set(bindVars)]; // Deduplicate
        };
        
        const updateBindNames = extractBindVariables(updateSql);
        const missingBinds = [];
        for (const bindName of updateBindNames) {
          if (binds[bindName] === undefined) {
            missingBinds.push(bindName);
          }
        }
        if (missingBinds.length > 0) {
          const errorMsg = `Missing bind variable(s) in UPDATE: ${missingBinds.join(', ')}. Available binds: ${Object.keys(binds).join(', ')}. UPDATE SQL: ${updateSql}`;
          console.error('OracleStorageAdapter ERROR:', errorMsg);
          throw new Parse.Error(
            Parse.Error.INTERNAL_SERVER_ERROR,
            errorMsg
          );
        }
        
        try {
          await connection.execute(updateSql, binds);
        } catch (updateError) {
          // Try to recompile array_contains if it's invalid
          const recompiled = await recompileArrayContainsIfNeeded(updateError, updateSql);
          if (recompiled) {
            // Retry the UPDATE after recompiling
            try {
              await connection.execute(updateSql, binds);
            } catch (retryError) {
              throw retryError;
            }
          } else {
            throw updateError;
          }
        }
      }

      // Fetch the updated rows using the same query
      // Use the same filtered binds for the SELECT
      
      const updatedResult = await connection.execute(selectSql, selectBinds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      // If no rows found after update (shouldn't happen since we checked before, but be safe)
      if (updatedResult.rows.length === 0) {
        if (shouldCloseConnection) {
          await connection.rollback();
        }
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
      }

      if (shouldCloseConnection) {
        await connection.commit();
      }

      // Convert Oracle rows to Parse objects
      const mappedResults = updatedResult.rows.map(obj => {
        try {
          return this.oracleObjectToParseObject(className, obj, schema);
        } catch (error) {
          debug('Error converting Oracle object to Parse object:', error);
          throw error;
        }
      });

      // Filter out any undefined/null results (shouldn't happen, but be safe)
      const validResults = mappedResults.filter(result => result != null);
      if (validResults.length === 0) {
        if (shouldCloseConnection) {
          await connection.rollback();
        }
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found after conversion.');
      }

      return validResults;
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      
      // If it's already a Parse.Error, re-throw it
      if (error instanceof Parse.Error) {
        throw error;
      }
      
      // Handle Oracle-specific errors
      if (error.errorNum) {
        // ORA-00001: unique constraint violated
        if (error.errorNum === 1) {
          const err = new Parse.Error(
            Parse.Error.DUPLICATE_VALUE,
            'A duplicate value for a field with unique values was provided'
          );
          err.underlyingError = error;
          throw err;
        }
        // ORA-00942: table or view does not exist
        if (error.errorNum === 942) {
          throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
        }
      }
      
      // For other errors, wrap in INTERNAL_SERVER_ERROR with more details
      debug('Error in updateObjectsByQuery:', error);
      const internalError = new Parse.Error(
        Parse.Error.INTERNAL_SERVER_ERROR,
        error.message || 'Internal server error'
      );
      internalError.underlyingError = error;
      throw internalError;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  // Hopefully, we can get rid of this. It's only used for config and hooks.
  async upsertOneObject(
    className: string,
    schema: SchemaType,
    query: QueryType,
    update: any,
    transactionalSession: ?any
  ) {
    await this._pgp;
    debug('upsertOneObject');
    const createValue = Object.assign({}, query, update);
    return this.createObject(className, schema, createValue, transactionalSession).catch(error => {
      // ignore duplicate value errors as it's upsert
      if (error.code !== Parse.Error.DUPLICATE_VALUE) {
        throw error;
      }
      return this.findOneAndUpdate(className, schema, query, update, transactionalSession);
    });
  }

  async find(className: string, schema: SchemaType, query: QueryType, options) {
    debug('find', className);

    const { skip, limit, sort, keys, caseInsensitive, explain } = options;
    
    const where = buildWhereClause({ schema, query, caseInsensitive });
    const wherePattern = where.pattern ? `WHERE ${where.pattern}` : '';

    let sortPattern = '';
    if (sort && Object.keys(sort).length > 0) {
      const sorting = Object.keys(sort)
        .map(key => `"${key}" ${sort[key] === 1 ? 'ASC' : 'DESC'}`)
        .join(', ');
      sortPattern = `ORDER BY ${sorting}`;
    }
    if (where.sorts?.length > 0) {
      sortPattern = `ORDER BY ${where.sorts.join(', ')}`;
    }

    let columns = '*';
    if (keys) {
      const filteredKeys = keys.reduce((memo, key) => {
        if (key === 'ACL') {
          memo.push('_rperm', '_wperm');
        } else if (
          key.length > 0 &&
          ((schema.fields[key] && schema.fields[key].type !== 'Relation') || key === '$score')
        ) {
          memo.push(key);
        }
        return memo;
      }, []);

      columns = filteredKeys
        .map(key => (key === '$score' ? 'SCORE(1) as score' : `"${key}"`))
        .join(', ');
    }

    let paginationPattern = '';
    if (skip !== undefined || limit !== undefined) {
      const offsetValue = skip || 0;
      paginationPattern =
        limit !== undefined
          ? `OFFSET ${offsetValue} ROWS FETCH NEXT ${limit} ROWS ONLY`
          : `OFFSET ${offsetValue} ROWS`;
    }

    const dataQuery = `SELECT ${columns} FROM "${className}" ${wherePattern} ${sortPattern} ${paginationPattern}`.trim();

    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      // Log SQL before execution for debugging
      const finalQuery = explain ? this.createExplainableQuery(dataQuery) : dataQuery;
      console.error(`[SQL DEBUG] About to execute FIND query for class "${className}":`);
      console.error(`  SQL: ${finalQuery}`);
      console.error(`  Binds:`, JSON.stringify(where.binds || {}, null, 2));
      
      const result = await connection.execute(
        finalQuery,
        where.binds || {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      if (explain) {
        return result.rows;
      }

      const mappedResults = result.rows.map(obj => {
        try {
          return this.oracleObjectToParseObject(className, obj, schema);
        } catch (conversionError) {
          debug('Error converting Oracle object to Parse object:', conversionError);
          throw conversionError;
        }
      });

      return mappedResults;
    } catch (error) {
      if (error.errorNum === 942) {
        return [];
      }
      
      // Enhanced error logging for SQL syntax errors
      if (error.errorNum === 907 || (error.message && error.message.includes('ORA-00907'))) {
        const finalQuery = explain ? this.createExplainableQuery(dataQuery) : dataQuery;
        console.error(`[CRITICAL] SQL syntax error (ORA-00907) in find for class "${className}":`);
        console.error(`  SQL: ${finalQuery}`);
        console.error(`  Binds:`, JSON.stringify(where.binds || {}, null, 2));
        console.error(`  Where pattern: ${wherePattern}`);
        console.error(`  Sort pattern: ${sortPattern}`);
        console.error(`  Pagination pattern: ${paginationPattern}`);
      }
      
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeError) {
          debug('Error closing connection in find:', closeError);
        }
      }
    }
  }
  // Converts from a Oracle-format object to a REST-format object.
  // Does not strip out anything based on a lack of authentication.
  oracleObjectToParseObject(className: string, object: any, schema: any) {
    Object.keys(schema.fields).forEach(fieldName => {
      if (schema.fields[fieldName].type === 'Pointer' && object[fieldName]) {
        object[fieldName] = {
          objectId: object[fieldName],
          __type: 'Pointer',
          className: schema.fields[fieldName].targetClass,
        };
      }
      if (schema.fields[fieldName].type === 'Relation') {
        object[fieldName] = {
          __type: 'Relation',
          className: schema.fields[fieldName].targetClass,
        };
      }
      if (object[fieldName] && schema.fields[fieldName].type === 'GeoPoint') {
        object[fieldName] = {
          __type: 'GeoPoint',
          latitude: object[fieldName].y,
          longitude: object[fieldName].x,
        };
      }
      if (object[fieldName] && schema.fields[fieldName].type === 'Polygon') {
        let coords = new String(object[fieldName]);
        coords = coords.substring(2, coords.length - 2).split('),(');
        const updatedCoords = coords.map(point => {
          return [parseFloat(point.split(',')[1]), parseFloat(point.split(',')[0])];
        });
        object[fieldName] = {
          __type: 'Polygon',
          coordinates: updatedCoords,
        };
      }
      if (object[fieldName] && schema.fields[fieldName].type === 'File') {
        object[fieldName] = {
          __type: 'File',
          name: object[fieldName],
        };
      }
    });
    //TODO: remove this reliance on the mongo format. DB adapter shouldn't know there is a difference between created at and any other date field.
    if (object.createdAt) {
      object.createdAt = object.createdAt.toISOString();
    }
    if (object.updatedAt) {
      object.updatedAt = object.updatedAt.toISOString();
    }
    if (object.expiresAt) {
      object.expiresAt = {
        __type: 'Date',
        iso: object.expiresAt.toISOString(),
      };
    }
    if (object._email_verify_token_expires_at) {
      object._email_verify_token_expires_at = {
        __type: 'Date',
        iso: object._email_verify_token_expires_at.toISOString(),
      };
    }
    if (object._account_lockout_expires_at) {
      object._account_lockout_expires_at = {
        __type: 'Date',
        iso: object._account_lockout_expires_at.toISOString(),
      };
    }
    if (object._perishable_token_expires_at) {
      object._perishable_token_expires_at = {
        __type: 'Date',
        iso: object._perishable_token_expires_at.toISOString(),
      };
    }
    if (object._password_changed_at) {
      object._password_changed_at = {
        __type: 'Date',
        iso: object._password_changed_at.toISOString(),
      };
    }

    for (const fieldName in object) {
      if (object[fieldName] === null) {
        delete object[fieldName];
      }
      if (object[fieldName] instanceof Date) {
        object[fieldName] = {
          __type: 'Date',
          iso: object[fieldName].toISOString(),
        };
      }
      // Parse JSON strings for permission fields
      if (
        (fieldName === '_rperm' || fieldName === '_wperm') &&
        typeof object[fieldName] === 'string'
      ) {
        try {
          object[fieldName] = JSON.parse(object[fieldName]);
        } catch (e) {
          // If parsing fails, keep as string (fallback)
        }
      }
      // Parse JSON strings for Array and Object types
      if (schema.fields[fieldName]) {
        const fieldType = schema.fields[fieldName].type;
        if ((fieldType === 'Array' || fieldType === 'Object') && typeof object[fieldName] === 'string') {
          try {
            const parsed = JSON.parse(object[fieldName]);
            object[fieldName] = parsed;
            // Ensure Object types are not arrays after parsing
            if (fieldType === 'Object' && Array.isArray(parsed)) {
              // If parsed JSON is an array but should be an object, convert to empty object
              // This handles edge cases where JSON might be malformed
              object[fieldName] = {};
            }
          } catch (e) {
            // If parsing fails, keep as string (fallback)
          }
        } else if (fieldType === 'Object' && Array.isArray(object[fieldName])) {
          // If Oracle returned an array for an Object type, convert to empty object
          // This should not happen if we stringify on insert, but handle it just in case
          object[fieldName] = {};
        }
      }
    }

    return object;
  }

  // Create a unique index. Unique indexes on nullable fields are not allowed. Since we don't
  // currently know which fields are nullable and which aren't, we ignore that criteria.
  // As such, we shouldn't expose this function to users of parse until we have an out-of-band
  // Way of determining if a field is nullable. Undefined doesn't count against uniqueness,
  // which is why we use sparse indexes.
  async ensureUniqueness(className: string, schema: SchemaType, fieldNames: string[]) {
    debug('ensureUniqueness', className, fieldNames);

    const constraintName = `${className}_unique_${fieldNames.sort().join('_')}`;
    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      const checkSql = `
      SELECT COUNT(*) as cnt
      FROM user_indexes
      WHERE UPPER(index_name) = UPPER(:indexName)
    `;

      const checkResult = await connection.execute(
        checkSql,
        { indexName: constraintName },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      if (checkResult.rows[0].CNT > 0) {
        debug(`Index ${constraintName} already exists`);
        return;
      }

      // Check for duplicates before attempting to create the unique index
      // ORA-01452 occurs when trying to create a unique index on columns with duplicate values
      const columnsList = fieldNames.map(field => `"${field}"`).join(', ');
      // For multi-column unique indexes, check for duplicate combinations
      // Use a subquery to count rows with duplicate combinations
      // Only check non-NULL combinations since NULLs are considered distinct in Oracle unique indexes
      const whereConditions = fieldNames.map(field => `"${field}" IS NOT NULL`).join(' AND ');
      const checkDuplicatesSql = `
        SELECT COUNT(*) as duplicate_count
        FROM (
          SELECT ${columnsList}, COUNT(*) as cnt
          FROM "${className}"
          ${whereConditions ? `WHERE ${whereConditions}` : ''}
          GROUP BY ${columnsList}
          HAVING COUNT(*) > 1
        )
      `;

      try {
        const duplicateCheckResult = await connection.execute(
          checkDuplicatesSql,
          {},
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        const duplicateCount = duplicateCheckResult.rows[0].DUPLICATE_COUNT;

        // If there are duplicates, we cannot create a unique index
        if (duplicateCount > 0) {
          debug(
            `Cannot create unique index ${constraintName} on ${className}: ${duplicateCount} duplicate value(s) found`
          );
          // No commit needed - we only performed SELECT queries
          return;
        }
      } catch (checkError) {
        // ORA-00942: table or view does not exist - table might not exist yet, continue
        if (checkError.errorNum === 942) {
          debug(`Table ${className} does not exist, skipping duplicate check`);
        } else {
          // For other errors, log but continue - we'll catch ORA-01452 during index creation
          debug(`Error checking for duplicates: ${checkError.message}`);
        }
      }

      const createIndexSql = `CREATE UNIQUE INDEX "${constraintName}" ON "${className}" (${columnsList})`;

      try {
        await connection.execute(createIndexSql);
        await connection.commit();
        debug(`Created unique index: ${constraintName}`);
      } catch (createError) {
        // ORA-00955: name is already used by an existing object
        // This can happen in race conditions where the index was created between our check and creation
        if (createError.errorNum === 955) {
          debug(`Index ${constraintName} already exists (race condition)`);
          // No commit needed - index creation failed, nothing to commit
          return;
        }
        // ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found
        // This can happen if duplicates were inserted between our check and index creation
        if (createError.errorNum === 1452) {
          debug(
            `Cannot create unique index ${constraintName} on ${className}: duplicate values found. Index creation skipped.`
          );
          // No commit needed - index creation failed, nothing to commit
          return;
        }
        throw createError;
      }
    } catch (error) {
      await connection.rollback();

      if (error.errorNum === 1) {
        throw new Parse.Error(
          Parse.Error.DUPLICATE_VALUE,
          'A duplicate value for a field with unique values was provided'
        );
      }

      throw error;
    } finally {
      await connection.close();
    }
  }

  // Executes a count.
  async count(
    className: string,
    schema: SchemaType,
    query: QueryType,
    readPreference?: string,
    estimate: boolean = true
  ) {
    debug('count', className);

    const where = buildWhereClause({ schema, query, caseInsensitive: false });
    const wherePattern = where.pattern ? `WHERE ${where.pattern}` : '';

    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      let sql;
      let binds;

      if (where.pattern || !estimate) {
        sql = `SELECT COUNT(*) as cnt FROM "${className}" ${wherePattern}`;
        binds = where.binds || {};
      } else {
        sql = `
        SELECT num_rows as approximate_row_count
        FROM user_tables
        WHERE table_name = :tableName
      `;
        binds = { tableName: className.toUpperCase() };
      }

      const result = await connection.execute(sql, binds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      if (result.rows.length === 0) {
        return 0;
      }

      const row = result.rows[0];
      return Number(row.APPROXIMATE_ROW_COUNT || row.CNT) || 0;
    } catch (error) {
      if (error.errorNum === 942) {
        return 0;
      }
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeError) {
          debug('Error closing connection in count:', closeError);
        }
      }
    }
  }

  async distinct(className, schema, query, fieldName) {
    debug('distinct', className, fieldName);

    const isNested = fieldName.indexOf('.') >= 0;
    const isArrayField = schema.fields?.[fieldName]?.type === 'Array';
    const isPointerField = schema.fields?.[fieldName]?.type === 'Pointer';

    const where = buildWhereClause({ schema, query, caseInsensitive: false });
    const wherePattern = where.pattern ? `WHERE ${where.pattern}` : '';

    const pool = await this._pgp;
    const connection = await pool.getConnection();

    try {
      let sql;

      if (isNested) {
        const [column, child] = fieldName.split('.');
        sql = `
        SELECT DISTINCT JSON_VALUE("${column}", '$.${child}') as value
        FROM "${className}"
        ${wherePattern}
      `;
      } else if (isArrayField) {
        sql = `
        SELECT DISTINCT jt.value
        FROM "${className}" t,
        JSON_TABLE(t."${fieldName}", '$[*]' 
          COLUMNS (value VARCHAR2(4000) PATH '$')
        ) jt
        ${wherePattern}
      `;
      } else {
        sql = `SELECT DISTINCT "${fieldName}" as value FROM "${className}" ${wherePattern}`;
      }

      const result = await connection.execute(sql, where.binds || {}, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      let results = result.rows.filter(row => row.VALUE !== null).map(row => row.VALUE);

      if (isPointerField && !isNested) {
        results = results.map(objectId => ({
          __type: 'Pointer',
          className: schema.fields[fieldName].targetClass,
          objectId: objectId,
        }));
      }

      return results.map(object => {
        try {
          return this.oracleObjectToParseObject(className, object, schema);
        } catch (conversionError) {
          debug('Error converting Oracle object to Parse object in distinct:', conversionError);
          throw conversionError;
        }
      });
    } catch (error) {
      if (error.errorNum === 904) {
        return [];
      }
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeError) {
          debug('Error closing connection in distinct:', closeError);
        }
      }
    }
  }

  async aggregate(
    className: string,
    schema: any,
    pipeline: any,
    readPreference?: string,
    hint?: any,
    explain?: boolean
  ) {
    debug('aggregate', className);

    const binds = {};
    let bindIndex = 0;
    const getBindName = () => `val${bindIndex++}`;

    let columns = [];
    let countField = null;
    let groupValues = null;
    let wherePattern = '';
    let limitPattern = '';
    let skipPattern = '';
    let sortPattern = '';
    let groupPattern = '';

    for (let i = 0; i < pipeline.length; i++) {
      const stage = pipeline[i];

      if (stage.$group) {
        for (const field in stage.$group) {
          const value = stage.$group[field];
          if (value === null || value === undefined) {
            continue;
          }

          if (field === '_id' && typeof value === 'string' && value !== '') {
            const fieldName = transformAggregateField(value);
            columns.push(`"${fieldName}" AS "objectId"`);
            groupPattern = `GROUP BY "${fieldName}"`;
            continue;
          }

          if (field === '_id' && typeof value === 'object' && Object.keys(value).length !== 0) {
            groupValues = value;
            const groupByFields = [];

            for (const alias in value) {
              if (typeof value[alias] === 'string' && value[alias]) {
                const source = transformAggregateField(value[alias]);
                if (!groupByFields.includes(`"${source}"`)) {
                  groupByFields.push(`"${source}"`);
                }
                columns.push(`"${source}" AS "${alias}"`);
              } else {
                const operation = Object.keys(value[alias])[0];
                const source = transformAggregateField(value[alias][operation]);

                if (mongoAggregateToOracle[operation]) {
                  if (!groupByFields.includes(`"${source}"`)) {
                    groupByFields.push(`"${source}"`);
                  }
                  columns.push(
                    `CAST(EXTRACT(${mongoAggregateToOracle[operation]} FROM "${source}") AS NUMBER) AS "${alias}"`
                  );
                }
              }
            }

            groupPattern = `GROUP BY ${groupByFields.join(', ')}`;
            continue;
          }

          if (typeof value === 'object') {
            // $sum
            if (value.$sum) {
              if (typeof value.$sum === 'string') {
                const fieldName = transformAggregateField(value.$sum);
                columns.push(`SUM("${fieldName}") AS "${field}"`);
              } else {
                countField = field;
                columns.push(`COUNT(*) AS "${field}"`);
              }
            }
            // $max
            if (value.$max) {
              const fieldName = transformAggregateField(value.$max);
              columns.push(`MAX("${fieldName}") AS "${field}"`);
            }
            // $min
            if (value.$min) {
              const fieldName = transformAggregateField(value.$min);
              columns.push(`MIN("${fieldName}") AS "${field}"`);
            }
            // $avg
            if (value.$avg) {
              const fieldName = transformAggregateField(value.$avg);
              columns.push(`AVG("${fieldName}") AS "${field}"`);
            }
          }
        }
      } else {
        columns.push('*');
      }

      if (stage.$project) {
        if (columns.includes('*')) {
          columns = [];
        }
        for (const field in stage.$project) {
          const value = stage.$project[field];
          if (value === 1 || value === true) {
            columns.push(`"${field}"`);
          }
        }
      }

      if (stage.$match) {
        const patterns = [];
        const orOrAnd = stage.$match.$or ? ' OR ' : ' AND ';

        if (stage.$match.$or) {
          const collapse = {};
          stage.$match.$or.forEach(element => {
            for (const key in element) {
              collapse[key] = element[key];
            }
          });
          stage.$match = collapse;
        }

        for (let field in stage.$match) {
          const value = stage.$match[field];
          if (field === '_id') {
            field = 'objectId';
          }

          const matchPatterns = [];

          Object.keys(ParseToOracleComparator).forEach(cmp => {
            if (value[cmp] !== undefined) {
              const oracleComparator = ParseToOracleComparator[cmp];
              const bindName = getBindName();
              binds[bindName] = toOracleValue(value[cmp]);
              matchPatterns.push(`"${field}" ${oracleComparator} :${bindName}`);
            }
          });

          if (matchPatterns.length > 0) {
            patterns.push(`(${matchPatterns.join(' AND ')})`);
          } else if (schema.fields[field]?.type && matchPatterns.length === 0) {
            const bindName = getBindName();
            binds[bindName] = value;
            patterns.push(`"${field}" = :${bindName}`);
          }
        }

        wherePattern = patterns.length > 0 ? `WHERE ${patterns.join(` ${orOrAnd} `)}` : '';
      }

      // $limit
      if (stage.$limit) {
        limitPattern = `FETCH FIRST ${stage.$limit} ROWS ONLY`;
      }

      // $skip
      if (stage.$skip) {
        skipPattern = `OFFSET ${stage.$skip} ROWS`;
      }

      if (stage.$sort) {
        const sort = stage.$sort;
        const keys = Object.keys(sort);
        const sorting = keys
          .map(key => {
            const transformer = sort[key] === 1 ? 'ASC' : 'DESC';
            return `"${key}" ${transformer}`;
          })
          .join(', ');
        sortPattern = sorting.length > 0 ? `ORDER BY ${sorting}` : '';
      }
    }

    if (groupPattern) {
      columns = columns.filter(col => col.trim() !== '*');
    }

    const columnsList = columns.filter(Boolean).join(', ') || '*';
    const sql = `SELECT ${columnsList} FROM "${className}" ${wherePattern} ${groupPattern} ${sortPattern} ${skipPattern} ${limitPattern}`.trim();

    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();
    try {
      const result = await connection.execute(
        explain ? this.createExplainableQuery(sql) : sql,
        binds,
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      if (explain) {
        return result.rows;
      }

      const results = result.rows.map(object => {
        try {
          return this.oracleObjectToParseObject(className, object, schema);
        } catch (conversionError) {
          debug('Error converting Oracle object to Parse object in aggregate:', conversionError);
          throw conversionError;
        }
      });

      results.forEach(result => {
        if (!Object.prototype.hasOwnProperty.call(result, 'objectId')) {
          result.objectId = null;
        }

        if (groupValues) {
          result.objectId = {};
          for (const key in groupValues) {
            result.objectId[key] = result[key];
            delete result[key];
          }
        }

        if (countField) {
          result[countField] = parseInt(result[countField], 10);
        }
      });

      return results;
    } catch (error) {
      debug('Aggregate error:', error);
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close(); // always release the connection back to the pool
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  async performInitialization({ VolatileClassesSchemas }: any) {
    // TODO: This method needs to be rewritten to make proper use of connections (@vitaly-t)
    debug('performInitialization');
    await this._ensureSchemaCollectionExists();
    const promises = VolatileClassesSchemas.map(schema => {
      return this.createTable(schema.className, schema)
        .catch(err => {
          if (
            err.code === OracleDuplicateRelationError ||
            err.code === Parse.Error.INVALID_CLASS_NAME
          ) {
            return Promise.resolve();
          }
          throw err;
        })
        .then(() => this.schemaUpgrade(schema.className, schema));
    });

    await this._setDateFormats();

    promises.push(this._listenToSchema());
    promises.push(this._installOracleFunctions());

    const start = Date.now();
    await Promise.all(promises).catch(error => {
      /* eslint-disable no-console */
      console.error(error);
    });

    debug(`initializationDone in ${Date.now() - start}`);
  }

  async _installOracleFunctions() {
    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    connection.execute(sql.misc.jsonObjectSetKeys);
    connection.execute(sql.array.add);
    connection.execute(sql.array.addUnique);
    connection.execute(sql.array.remove);
    connection.execute(sql.array.containsAll);
    connection.execute(sql.array.containsAllRegex);
    connection.execute(sql.array.contains);

    connection.close();
  }

  async createIndexes(className: string, indexes: any, conn?: any): Promise<void> {
    debug('createIndexes', className, indexes);

    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      for (const index of indexes) {
        const indexName = index.name || `${className}_${index.key}_idx`;
        const columnName = index.key;

        const createIndexSql = `CREATE INDEX IF NOT EXISTS "${indexName}" ON "${className}" ("${columnName}")`;

        try {
          await connection.execute(createIndexSql);
          debug(`Created index: ${indexName}`);
        } catch (error) {
          // ORA-00955: name is already used by an existing object
          // Oracle 23c IF NOT EXISTS should prevent this, but keep for compatibility
          if (error.errorNum === 955) {
            debug(`Index ${indexName} already exists, skipping`);
            continue;
          }
          throw error;
        }
      }

      if (shouldCloseConnection) {
        await connection.commit();
      }
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async createIndexesIfNeeded(
    className: string,
    fieldName: string,
    type: any,
    conn?: any
  ): Promise<void> {
    debug('createIndexesIfNeeded', className, fieldName);

    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      const indexName = `${className}_${fieldName}_idx`;
      const createIndexSql = `CREATE INDEX IF NOT EXISTS "${indexName}" ON "${className}" ("${fieldName}")`;

      try {
        await connection.execute(createIndexSql);
        debug(`Created index: ${indexName}`);
      } catch (error) {
        // ORA-00955: name is already used by an existing object
        // Oracle 23c IF NOT EXISTS should prevent this, but keep for compatibility
        if (error.errorNum !== 955) {
          throw error;
        }
        debug(`Index ${indexName} already exists`);
      }

      if (shouldCloseConnection) {
        await connection.commit();
      }
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async dropIndexes(className: string, indexes: any, conn?: any): Promise<void> {
    debug('dropIndexes', className, indexes);

    const pool = await this._pgp;
    const connection = conn || (await pool.getConnection());
    const shouldCloseConnection = !conn;

    try {
      for (const indexName of indexes) {
        const dropIndexSql = `DROP INDEX "${indexName}"`;

        try {
          await connection.execute(dropIndexSql);
          debug(`Dropped index: ${indexName}`);
        } catch (error) {
          // ORA-01418: specified index does not exist
          if (error.errorNum === 1418) {
            debug(`Index ${indexName} does not exist, skipping`);
            continue;
          }
          throw error;
        }
      }

      if (shouldCloseConnection) {
        await connection.commit();
      }
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async getIndexes(className: string) {
    debug('getIndexes', className);

    let connection;
    try {
      const sql = `
          SELECT 
              i.index_name as "indexname",
              i.table_name as "tablename",
              c.column_name as "columnname",
              i.uniqueness as "unique",
              e.column_expression as "expression"
          FROM user_ind_columns c
                   LEFT JOIN user_indexes i
                        ON i.index_name = c.index_name
                   LEFT JOIN user_ind_expressions e
                        ON e.index_name = i.index_name
          WHERE i.table_name = :tableName
          ORDER BY i.index_name, c.column_position
    `;

    const pool = await this._pgp;
    connection = await pool.getConnection();

      const result = await connection.execute(
        sql,
        { tableName: className },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      return result.rows;
    } catch (error) {
      debug('Error getting indexes:', error);
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close(); // always release the connection back to the pool
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  async updateSchemaWithIndexes(): Promise<void> {
    return Promise.resolve();
  }

  // Used for testing purposes
  async updateEstimatedCount(className: string) {
    debug('updateEstimatedCount', className);

    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();
    try {
      const analyzeSql = `
      BEGIN
        DBMS_STATS.GATHER_TABLE_STATS(
          ownname => USER,
          tabname => '${className.toUpperCase()}',
          estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
          method_opt => 'FOR ALL COLUMNS SIZE AUTO'
        );
      END;
    `;

      await connection.execute(analyzeSql);

      debug(`Updated statistics for ${className}`);
    } catch (error) {
      debug('Error updating estimated count:', error);
      throw error;
    } finally {
      if (connection) {
        try {
          await connection.close(); // always release the connection back to the pool
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  async createTransactionalSession(): Promise<any> {
    debug('createTransactionalSession');

    await this._pgp;
    const pool = await this._pgp;
    const connection = await pool.getConnection();

    const transactionalSession = {
      connection: connection,
      operations: [],
      committed: false,
      aborted: false,
    };

    if (connection) {
      await connection.close();
    }

    return transactionalSession;
  }

  async commitTransactionalSession(transactionalSession: any): Promise<void> {
    debug('commitTransactionalSession');

    if (transactionalSession.aborted) {
      throw new Error('Cannot commit an aborted transaction');
    }

    if (transactionalSession.committed) {
      debug('Transaction already committed');
      return;
    }

    try {
      for (const operation of transactionalSession.operations) {
        await operation(transactionalSession.connection);
      }

      await transactionalSession.connection.commit();
      transactionalSession.committed = true;

      debug('Transaction committed successfully');
    } catch (error) {
      debug('Error committing transaction:', error);
      await transactionalSession.connection.rollback();
      throw error;
    } finally {
      await transactionalSession.connection.close();
    }
  }

  async abortTransactionalSession(transactionalSession: any): Promise<void> {
    debug('abortTransactionalSession');

    if (transactionalSession.committed) {
      throw new Error('Cannot abort a committed transaction');
    }

    if (transactionalSession.aborted) {
      debug('Transaction already aborted');
      return;
    }

    try {
      await transactionalSession.connection.rollback();
      transactionalSession.aborted = true;

      debug('Transaction aborted successfully');
    } catch (error) {
      debug('Error aborting transaction:', error);
      throw error;
    } finally {
      await transactionalSession.connection.close();
    }
  }

  async ensureIndex(
    className: string,
    schema: SchemaType,
    fieldNames: string[],
    indexName?: string,
    caseInsensitive: boolean = false,
    options: any = {}
  ): Promise<any> {
    debug('ensureIndex', className, fieldNames);

    const pool = await this._pgp;
    const connection = options.conn || (await pool.getConnection());
    const shouldCloseConnection = !options.conn;

    try {
      const defaultIndexName = `parse_default_${fieldNames.sort().join('_')}`;
      const finalIndexName = indexName || defaultIndexName;

      let columnsList;
      if (caseInsensitive) {
        columnsList = fieldNames.map(field => `LOWER("${field}")`).join(', ');
      } else {
        columnsList = fieldNames.map(field => `"${field}"`).join(', ');
      }

      const createIndexSql = `CREATE INDEX IF NOT EXISTS "${finalIndexName}" ON "${className}" (${columnsList})`;

      if (options.setIdempotencyFunction) {
        await this.ensureIdempotencyFunctionExists(options);
      }

      try {
        await connection.execute(createIndexSql);
        debug(`Created index: ${finalIndexName}`);
      } catch (error) {
        // ORA-00955: name is already used by an existing object
        if (error.errorNum === 955 && error.message.includes(finalIndexName)) {
          debug(`Index ${finalIndexName} already exists, ignoring`);
        }
        // ORA-00001: unique constraint violated
        else if (error.errorNum === 1 && error.message.includes(finalIndexName)) {
          throw new Parse.Error(
            Parse.Error.DUPLICATE_VALUE,
            'A duplicate value for a field with unique values was provided'
          );
        } else {
          throw error;
        }
      }

      if (shouldCloseConnection) {
        await connection.commit();
      }
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async deleteIdempotencyFunction(options: any = {}): Promise<any> {
    debug('deleteIdempotencyFunction');

    const pool = await this._pgp;
    const connection = options.conn || (await pool.getConnection());
    const shouldCloseConnection = !options.conn;

    try {
      const dropSql = `
      BEGIN
        EXECUTE IMMEDIATE 'DROP PROCEDURE idempotency_delete_expired_records';
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE != -4043 THEN -- ORA-04043: object does not exist
            RAISE;
          END IF;
      END;
    `;

      await connection.execute(dropSql);

      if (shouldCloseConnection) {
        await connection.commit();
      }

      debug('Idempotency function deleted');
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }

  async ensureIdempotencyFunctionExists(options: any = {}): Promise<any> {
    debug('ensureIdempotencyFunctionExists');

    const pool = await this._pgp;
    const connection = options.conn || (await pool.getConnection());
    const shouldCloseConnection = !options.conn;

    try {
      const ttlSeconds = options.ttl || 60;

      const createProcedureSql = `
      CREATE OR REPLACE PROCEDURE idempotency_delete_expired_records
      IS
      BEGIN
        DELETE FROM "_Idempotency"
        WHERE expire < SYSTIMESTAMP - INTERVAL '${ttlSeconds}' SECOND;
        
        COMMIT;
      END;
    `;

      await connection.execute(createProcedureSql);

      if (shouldCloseConnection) {
        await connection.commit();
      }

      debug(`Idempotency function created with TTL: ${ttlSeconds} seconds`);
    } catch (error) {
      if (shouldCloseConnection) {
        await connection.rollback();
      }
      throw error;
    } finally {
      if (shouldCloseConnection && connection) {
        await connection.close();
      }
    }
  }
}

function convertPolygonToSQL(polygon) {
  if (polygon.length < 3) {
    throw new Parse.Error(Parse.Error.INVALID_JSON, `Polygon must have at least 3 values`);
  }
  if (
    polygon[0][0] !== polygon[polygon.length - 1][0] ||
    polygon[0][1] !== polygon[polygon.length - 1][1]
  ) {
    polygon.push(polygon[0]);
  }
  const unique = polygon.filter((item, index, ar) => {
    let foundIndex = -1;
    for (let i = 0; i < ar.length; i += 1) {
      const pt = ar[i];
      if (pt[0] === item[0] && pt[1] === item[1]) {
        foundIndex = i;
        break;
      }
    }
    return foundIndex === index;
  });
  if (unique.length < 3) {
    throw new Parse.Error(
      Parse.Error.INTERNAL_SERVER_ERROR,
      'GeoJSON: Loop must have at least 3 different vertices'
    );
  }
  const points = polygon
    .map(point => {
      Parse.GeoPoint._validate(parseFloat(point[1]), parseFloat(point[0]));
      return `(${point[1]}, ${point[0]})`;
    })
    .join(', ');
  return `(${points})`;
}

function removeWhiteSpace(regex) {
  if (!regex.endsWith('\n')) {
    regex += '\n';
  }

  // remove non escaped comments
  return (
    regex
      .replace(/([^\\])#.*\n/gim, '$1')
      // remove lines starting with a comment
      .replace(/^#.*\n/gim, '')
      // remove non escaped whitespace
      .replace(/([^\\])\s+/gim, '$1')
      // remove whitespace at the beginning of a line
      .replace(/^\s+/, '')
      .trim()
  );
}

function processRegexPattern(s) {
  if (s && s.startsWith('^')) {
    // regex for startsWith
    return '^' + literalizeRegexPart(s.slice(1));
  } else if (s && s.endsWith('$')) {
    // regex for endsWith
    return literalizeRegexPart(s.slice(0, s.length - 1)) + '$';
  }

  // regex for contains
  return literalizeRegexPart(s);
}

function isStartsWithRegex(value) {
  if (!value || typeof value !== 'string' || !value.startsWith('^')) {
    return false;
  }

  const matches = value.match(/\^\\Q.*\\E/);
  return !!matches;
}

function isAllValuesRegexOrNone(values) {
  if (!values || !Array.isArray(values) || values.length === 0) {
    return true;
  }

  const firstValuesIsRegex = isStartsWithRegex(values[0].$regex);
  if (values.length === 1) {
    return firstValuesIsRegex;
  }

  for (let i = 1, length = values.length; i < length; ++i) {
    if (firstValuesIsRegex !== isStartsWithRegex(values[i].$regex)) {
      return false;
    }
  }

  return true;
}

function isAnyValueRegexStartsWith(values) {
  return values.some(function (value) {
    return isStartsWithRegex(value.$regex);
  });
}

function createLiteralRegex(remaining: string) {
  return remaining
    .split('')
    .map(c => {
      const regex = RegExp('[0-9 ]|\\p{L}', 'u'); // Support all Unicode letter chars
      if (c.match(regex) !== null) {
        // Don't escape alphanumeric characters
        return c;
      }
      // Escape everything else (single quotes with single quotes, everything else with a backslash)
      return c === `'` ? `''` : `\\${c}`;
    })
    .join('');
}

function literalizeRegexPart(s: string) {
  const matcher1 = /\\Q((?!\\E).*)\\E$/;
  const result1: any = s.match(matcher1);
  if (result1 && result1.length > 1 && result1.index > -1) {
    // Process Regex that has a beginning and an end specified for the literal text
    const prefix = s.substring(0, result1.index);
    const remaining = result1[1];

    return literalizeRegexPart(prefix) + createLiteralRegex(remaining);
  }

  // Process Regex that has a beginning specified for the literal text
  const matcher2 = /\\Q((?!\\E).*)$/;
  const result2: any = s.match(matcher2);
  if (result2 && result2.length > 1 && result2.index > -1) {
    const prefix = s.substring(0, result2.index);
    const remaining = result2[1];

    return literalizeRegexPart(prefix) + createLiteralRegex(remaining);
  }

  // Remove problematic chars from remaining text
  return (
    s
      // Remove all instances of \Q and \E
      .replace(/([^\\])(\\E)/, '$1')
      .replace(/([^\\])(\\Q)/, '$1')
      .replace(/^\\E/, '')
      .replace(/^\\Q/, '')
      // Ensure even number of single quote sequences by adding an extra single quote if needed;
      // this ensures that every single quote is escaped
      .replace(/'+/g, match => {
        return match.length % 2 === 0 ? match : match + "'";
      })
  );
}

const GeoPointCoder = {
  isValidJSON(value): boolean {
    return typeof value === 'object' && value !== null && value.__type === 'GeoPoint';
  },
};

export default OracleStorageAdapter;
