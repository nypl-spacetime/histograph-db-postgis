var R = require('ramda')
var pg = require('pg')
var normalize = require('histograph-uri-normalizer').normalize

// TODO: PG connection string from config
// var config = require('histograph-config')

// https://devcenter.heroku.com/articles/getting-started-with-nodejs#provision-a-database
var pgConString = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost/histograph'
function executeQuery (query, values, callback) {
  pg.connect(pgConString, function (err, client, done) {
    if (err) {
      callback(err)
    } else {
      client.query(query, values, function (err, result) {
        done()
        if (err) {
          callback(err)
        } else {
          callback(null, result.rows)
        }
      })
    }
  })
}

// TODO: tableName from config!
var tableName = 'pits'

var tableExists = `SELECT COUNT(*)
  FROM pg_catalog.pg_tables
  WHERE schemaname = 'public'
  AND tablename  = '${tableName}';
`

var createTable = `CREATE TABLE public.${tableName} (
    id text NOT NULL,
    dataset text NOT NULL,
    name text,
    type text,
    data jsonb,
    geometry geometry,
    CONSTRAINT ${tableName}_pkey PRIMARY KEY (id, dataset)
  );
  CREATE INDEX ${tableName}_gix ON ${tableName} USING GIST (geometry);
  CREATE INDEX ${tableName}_dataset ON ${tableName} (dataset);
  CREATE INDEX ${tableName}_type ON ${tableName} (type);
`

executeQuery(tableExists, null, function (err, rows) {
  if (err) {
    console.error('Error connecting to database:', err.message)
    process.exit(-1)
  } else {
    if (!(rows && rows[0].count === '1')) {
      console.log(`Table "${tableName}" does not exist - creating table...`)
      executeQuery(createTable, null, function (err) {
        if (err) {
          console.error('Error creating table:', err.message)
          process.exit(-1)
        }
      })
    }
  }
})

function escapeLiteral (str) {
  if (!str) {
    return 'NULL'
  }

  var hasBackslash = false
  var escaped = '\''

  for (var i = 0; i < str.length; i++) {
    var c = str[i]
    if (c === '\'') {
      escaped += c + c
    } else if (c === '\\') {
      escaped += c + c
      hasBackslash = true
    } else {
      escaped += c
    }
  }

  escaped += '\''

  if (hasBackslash === true) {
    escaped = ' E' + escaped
  }

  return escaped
}

function toRow (pit) {
  var id = normalize(pit.id || pit.uri, pit.dataset)

  return {
    id: `${escapeLiteral(id)}`,
    dataset: `'${pit.dataset}'`,
    name: `${escapeLiteral(pit.name)}`,
    type: `'${pit.type}'`,
    data: `${escapeLiteral(JSON.stringify(pit.data))}`,
    geometry: pit.geometry ? `ST_SetSRID(ST_GeomFromGeoJSON('${JSON.stringify(pit.geometry)}'), 4326)` : 'NULL'
  }
}

function createUpdateQuery (message) {
  var pit = Object.assign({dataset: message.dataset}, message.data)
  var row = toRow(pit)

  var columns = R.keys(row)
  var values = R.values(row)

  var query = `INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES (${values.join(', ')})
    ON CONFLICT (id, dataset)
    DO UPDATE SET
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      data = EXCLUDED.data,
      geometry = EXCLUDED.geometry;
  `

  return query
}

function deleteQuery (message) {
  var id = escapeLiteral(message.data.id || message.data.uri)
  var dataset = escapeLiteral(message.dataset)

  var query = `DELETE FROM ${tableName}
    WHERE
      id = ${id} AND
      dataset = ${dataset};
  `

  return query
}

var actionToQuery = {
  add: createUpdateQuery,
  update: createUpdateQuery,
  delete: deleteQuery
}

function messageToQuery (message) {
  return actionToQuery[message.action](message)
}

module.exports.bulk = function (messages, callback) {
  var queries = messages
    .filter((i) => i.type === 'pit')
    .map(messageToQuery)

  if (queries.length) {
    executeQuery(queries.join('\n'), null, function (err) {
      if (err) {
        callback(err)
      } else {
        console.log('PostGIS =>', messages.length)
        callback()
      }
    })
  } else {
    callback()
  }
}
