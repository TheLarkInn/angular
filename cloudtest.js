var google = require('googleapis');
var bigquery = google.bigquery('v2');
var Q = require('q');

var HEADER_FIELDS = [
  {
    "name": 'runId',
    "type": 'STRING',
    "description": 'uuid for the benchmark run'
  },
  {
    "name": 'index',
    "type": 'INTEGER',
    "description": 'index within the sample'
  },
  {
    "name": 'creationTime',
    "type": 'TIMESTAMP'
  },
  {
    "name": 'browser',
    "type": 'STRING',
    "description": 'navigator.platform'
  },
  {
    "name": 'forceGc',
    "type": 'BOOLEAN',
    "description": 'whether gc was forced at end of action'
  }
];

run({
  cloudReporter: {
    projectId: 'angular-perf',
    datasetId: 'benchmarks',
    auth: require(process.env.CLOUD_SECRET_PATH)
  },
  params: [{
    name: 'param1',
    value: 10
  }],
  metrics: ['metric1']
}).then(function() {
  console.log('done');
}, function(err) {
  console.log('error', err);
});

function run(benchmarkConfig) {
  var tableConfig = createTableConfig(benchmarkConfig, 'test');
  return authenticate(benchmarkConfig.cloudReporter.auth).then(function(authClient) {
    return getOrCreateTable(authClient, tableConfig);
  });
}

function createTableConfig(benchmarkConfig, tableId) {
  return {
    projectId: benchmarkConfig.cloudReporter.projectId,
    datasetId: benchmarkConfig.cloudReporter.datasetId,
    table: {
      id: tableId,
      fields: HEADER_FIELDS
        .concat(benchmarkConfig.params.map(function(param) {
          return {
            "name": 'p_'+param.name,
            "type": 'FLOAT'
          };
        }))
        .concat(benchmarkConfig.metrics.map(function(metricName) {
          return {
            "name": 'm_'+metricName,
            "type": 'FLOAT'
          };
        }))
    }
  };
}

function getOrCreateTable(authClient, tableConfig) {
  return getTable(authClient, tableConfig).then(null, function(err) {
    // create the table if it does not exist
    return createTable(authClient, tableConfig);
  });
}

function authenticate(authConfig) {
  var authClient = new google.auth.JWT(
    authConfig['client_email'],
    null,
    authConfig['private_key'],
    ['https://www.googleapis.com/auth/bigquery'],
    // User to impersonate (leave empty if no impersonation needed)
    null);

  var defer = Q.defer();
  authClient.authorize(makeNodeJsResolver(defer));
  return defer.promise.then(function() {
    return authClient;
  });
}

function getTable(authClient, tableConfig) {
  // see https://cloud.google.com/bigquery/docs/reference/v2/tables/get
  var params = {
    auth: authClient,
    projectId: tableConfig.projectId,
    datasetId: tableConfig.datasetId,
    tableId: tableConfig.table.id
  };
  var defer = Q.defer();
  bigquery.tables.get(params, makeNodeJsResolver(defer));
  return defer.promise;
}

function createTable(authClient, tableConfig) {
  // see https://cloud.google.com/bigquery/docs/reference/v2/tables
  // see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
  var params = {
    auth: authClient,
    projectId: tableConfig.projectId,
    datasetId: tableConfig.datasetId,
    resource: {
      "kind": "bigquery#table",
      "tableReference": {
        projectId: tableConfig.projectId,
        datasetId: tableConfig.datasetId,
        tableId: tableConfig.table.id
      },
      "schema": {
        "fields": tableConfig.table.fields
      }
    }
  };
  var defer = Q.defer();
  bigquery.tables.insert(params, makeNodeJsResolver(defer));
  return defer.promise;
}

function insertRows(authClient, tableConfig, rows) {
  // see https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
  var params = {
    auth: authClient,
    projectId: tableConfig.projectId,
    datasetId: tableConfig.datasetId,
    tableId: tableConfig.table.id,
    resource: {
      "kind": "bigquery#tableDataInsertAllRequest",
      "rows": rows.map(function(row) {
        return {
          json: row
        }
      })
    }
  };
  var defer = Q.defer();
  bigquery.tabledata.insertAll(params, makeNodeJsResolver(defer));
  return defer.promise.then(function(result) {
    if (result.insertErrors) {
      throw result.insertErrors.map(function(err) {
        return err.errors.map(function(err) {
          return err.message;
        }).join('\n');
      }).join('\n');
    }
  });
}

function makeNodeJsResolver(defer) {
  return function(err, result) {
    if (err) {
      // // Normalize errors messages from BigCloud so that they show up nicely
      // if (err.errors) {
      //   err = err.errors.map(function(err) {
      //     return err.message;
      //   }).join('\n');
      // }
      // Format errors in a nice way
      defer.reject(JSON.stringify(err, null, '  '));
    } else {
      defer.fulfill(result);
    }
  }
}
