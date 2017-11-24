var fs = require('fs');
var pg = require('pg');
var SparqlParser = require('sparqljs').Parser;
var parser = new SparqlParser();
var SparqlGenerator = require('sparqljs').Generator;
var generator = new SparqlGenerator();
var request = require('request');
var copyFrom = require('pg-copy-streams').from;

const config = {
    user: 'postgres',
    database: 'postgres',
    password: 'postgres',
    port: 5432
};

const pool = new pg.Pool(config);


queryPostgres = function (client, query) {
    return new Promise((resolve, reject) => {
        client.query(query, function (err, result) {
            if (err) {
                return reject(err);
            }
            return resolve(result.rows);
        });
    });
}

csvToPostgres = function (client, tableName, fileName) {
    return new Promise((resolve, reject) => {
        console.log("Loading... (" + tableName +")");
        var stream = client.query(copyFrom('COPY "' + tableName +'" FROM STDIN WITH (FORMAT csv)'));
        var fileStream = fs.createReadStream("./" + fileName);
        fileStream.on('error', function(error) { reject('Error:', error.message) });
        fileStream.pipe(stream)
            .on('finish', resolve())
            .on('error', function(error) { reject('Error:', error.message) });
    });
}

requestSPARQL = function (endpoint, query) {
    return new Promise((resolve, reject) => {
        console.log("Requesting... (" + endpoint +")");
        request({
            headers: {
              'Accept': "text/csv"
            },
            method: 'GET',
            url: endpoint + "?query=" + encodeURIComponent(query) + "&format=csv",
        }, function (error, response, body) {
            if (error) {
                return reject(error);
            }
            console.log("Done! (" + endpoint +")");
            return resolve(body);
        });
    });
}

createTableQuery = function (tableName, headArray) {
    var params = "";
    for (var i = 0; i < headArray.length; i++) {
        params += headArray[i] + " varchar,";
    }
    params = params.slice(0, -1);

    return `CREATE TABLE "${tableName}" (${params});`;
}

singleQueryRun = function (client, query, endpoint, done) {
    console.log("Asking " + endpoint + " for " + query);
    var q = parser.parse(query);
    var header = [];
    for (var i = 0; i < q.variables.length; i++) {
        header.push(q.variables[i].replace("?",""));
    }

    var randomName = new Date().toISOString();
    console.log(createTableQuery(randomName, header))
    queryPostgres(client, createTableQuery(randomName, header)).then(dbres => {
        return requestSPARQL(endpoint, query);
    }).then(csv => {
        fs.writeFileSync(randomName, csv);
        return csvToPostgres(client, randomName, randomName);
    }).then(function () {
        console.log("Finish!");
        done(randomName);
    }).catch(error => {
        console.error(error);
    })
}

standardResponseJSON = function (finalSelectHeader, postgresArray) {
    var resArray = [];
    for (var i = 0; i < postgresArray.length; i++) {
        var o = {};
        for (var j = 0; j < finalSelectHeader.length; j++) {
            o[finalSelectHeader[j]] = {};
            o[finalSelectHeader[j]]["type"] = "literal";
            o[finalSelectHeader[j]]["value"] = postgresArray[i][finalSelectHeader[j]];
        }
        resArray.push(o);
    }
    var replyObj = {};
    replyObj.head = {};
    replyObj.head.vars = finalSelectHeader;
    replyObj.results = {};
    replyObj.results.bindings = resArray;

    return replyObj;
}

joinAndResult = function (client, tableList, finalSelectHeader, done) {
    var commonVariable = "key"; //XXX

    var query = `
       SELECT ${finalSelectHeader}
       FROM "${tableList[0]}"
       INNER JOIN "${tableList[1]}" ON "${tableList[1]}".${commonVariable} = "${tableList[0]}".${commonVariable}
    `

    console.log(query);

    queryPostgres(client, query).then(dbres => {
        done(dbres);
    }).catch(error => {
        console.error(error);
    })
}

exports.main = function (serviceQuery, reply) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("Error in connecting with Postgres" + err);
        }

        var finalSelectHeader = [];
        var parsedQuery = parser.parse(serviceQuery);
        for (var i = 0; i < parsedQuery.variables.length; i++) {
            finalSelectHeader.push(parsedQuery.variables[i].replace("?",""));
        }

        var c = 0;
        var allTable = [];
        for (var i = 0; i < parsedQuery.where.length; i++) {
            var e = parsedQuery.where[i].name;
            var q = generator.stringify(parsedQuery.where[i]);
            singleQueryRun(client, q, e, function (table) {
                allTable.push(table);
                if (++c === parsedQuery.where.length) {
                    for (var j = 0; j < allTable.length; j++) {
                        fs.unlinkSync(allTable[j]);
                    }
                    joinAndResult(client, allTable, finalSelectHeader, function (res) {
                        reply(standardResponseJSON(finalSelectHeader, res));

                        var deleteQuery = 'DROP TABLE "' + allTable.toString().replace(/,/g,'","') + '"';
                        console.log(deleteQuery);
                        queryPostgres(client, deleteQuery).then(dbres => {
                            done();
                        }).catch(error => {
                            console.error(error);
                        })
                    });
                }
            });
        }

    });
}
