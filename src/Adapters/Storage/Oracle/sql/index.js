'use strict';
const fs = require('fs');
const path = require('path');

module.exports = {
  array: {
    add: loadSQL('array/add.sql'),
    addUnique: loadSQL('array/add-unique.sql'),
    contains: loadSQL('array/contains.sql'),
    containsAll: loadSQL('array/contains-all.sql'),
    containsAllRegex: loadSQL('array/contains-all-regex.sql'),
    remove: loadSQL('array/remove.sql'),
  },
  misc: {
    jsonObjectSetKeys: loadSQL('misc/json-object-set-keys.sql'),
  },
};

/**
 * @param {string} file - filepath of the SQL file relative to this module
 * @returns {string} - SQL file content
 */
function loadSQL(file) {
  try {
    const fullPath = path.join(__dirname, file);
    const sql = fs.readFileSync(fullPath, 'utf8');

    // Минифицируем SQL (убираем лишние пробелы и комментарии)
    return minifySQL(sql);
  } catch (error) {
    console.error(`Error loading SQL file: ${file}`, error);
    throw error;
  }
}

/**
 * @param {string} sql - source SQL
 * @returns {string} - minified SQL
 */
function minifySQL(sql) {
  return sql
    // Удаляем однострочные комментарии
    .replace(/--.*$/gm, '')
    // Удаляем многострочные комментарии
    .replace(/\/\*[\s\S]*?\*\//g, '')
    // Удаляем лишние пробелы и переносы строк
    .replace(/\s+/g, ' ')
    // Убираем пробелы в начале и конце
    .trim();
}
