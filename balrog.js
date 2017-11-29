var fs = require('fs');
var pg = require('pg');
var SparqlParser = require('sparqljs').Parser;
var parser = new SparqlParser();
var SparqlGenerator = require('sparqljs').Generator;
var generator = new SparqlGenerator();
var request = require('request');
var utils = require('./utils')

var currentID;

const config = {
    user: 'postgres',
    database: 'postgres',
    password: 'postgres',
    port: 5432
};

const pool = new pg.Pool(config);


checkQueryPattern = function (query) {
    if (query.where === undefined) return false;

    for (var i = 0; i < query.where.length; i++) {
        if (query.where[i].type !== "service") return false;
    }

    return true;
}

requestSPARQL = function (endpoint, query, mode) {
    return new Promise((resolve, reject) => {
        console.log("[" + currentID + "] Requesting data... (" + endpoint +") mode " + mode);

        var url = endpoint + "?query=" + encodeURIComponent(query);
        var accept = "application/json";

        /* Mode needed for get more endpoints work. Maybe the right mode for an
           endpoint can be saved to save time in future and to collect an
           interesting DB */
        if (mode === 1) url += "&format=json";
        if (mode === 2) accept = "text/csv";
        if (mode === 3) url += "&format=csv";

        request({
            headers: {
              'Accept': accept
            },
            method: 'GET',
            url: url
        }, function (error, response, body) {
            if (error) {
                return reject(error);
            }
            console.log("[" + currentID + "] Data arrived! (" + endpoint +")");
            try {
                var result = {};

                if (mode === 0 || mode === 1) {
                    result.body = JSON.parse(body);
                    result.type = "json";
                } else if (mode === 2 || mode === 3) {
                    //TODO test if body its a valid CSV
                    result.body = body;
                    result.type = "csv";
                }
                return resolve(result);
            } catch (e) {
                if (mode < 3) {
                    requestSPARQL(endpoint, query, ++mode).then(result => {
                        return resolve(result);
                    })
                } else {
                    return reject("Unable to get valid data from " + endpoint);
                }
            }
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

/* field, type, datatype
   test, literal, http://... */
insertDataTypeQuery = function (tableName, dataTypeObject) {
    var query = "";
    for (var i = 0; i < dataTypeObject.length; i++) {
        query += `INSERT INTO "${tableName}" (field, type, datatype) VALUES ('${dataTypeObject[i].field}', '${dataTypeObject[i].type}', '${dataTypeObject[i].datatype}'); `;
    }

    console.log(query)

    return query;
}

var dataTypeObject = []; //XXX better scope!!!

singleQueryRun = function (client, query, endpoint) {
    return new Promise((resolve, reject) => {
        console.log("[" + currentID + "] ASK " + endpoint + "\n" + query);
        var q;

        try {
            q = parser.parse(query);
        } catch (e) {
            reject(e);
        }

        var header = [];
        for (var i = 0; i < q.variables.length; i++) {
            header.push(q.variables[i].replace("?",""));
        }

        var randomName = new Date().toISOString();
        var currentTableCreate = createTableQuery(randomName, header);
        console.log("[" + currentID + "] " + currentTableCreate)

        utils.queryPostgres(client, currentTableCreate).then(dbres => {
            return requestSPARQL(endpoint, query, 0);
        }).then(sparqlResult => {
            /* JSON */
            if (sparqlResult.type = "json") {
                fs.writeFileSync(randomName, header.toString() + "\n");
                var dataVector = sparqlResult.body.results.bindings;
                for (var i = 0; i < dataVector.length; i++) {
                    var line = [];
                    for (var j = 0; j < header.length; j++) {
                        line.push(dataVector[i][header[j]].value);
                    }
                    var quotedAndCommaSeparated = '"' + line.join('","') + '"';
                    fs.appendFileSync(randomName, quotedAndCommaSeparated + "\n");
                }

                dataTypeObject = [];
                for (var key in dataVector[0]) {
                    var o = {};
                    o.field = key;
                    o.type = dataVector[0][key].type || null;
                    o.datatype = dataVector[0][key].datatype || null;
                    dataTypeObject.push(o);
                }

            /* CSV */
            } else if (sparqlResult.type = "csv") {
                fs.writeFileSync(randomName, sparqlResult.body);

                for (var i = 0; i < header.length; i++) {
                    var o = {};
                    o.field = header[i];
                    o.type = "literal";
                    o.datatype = null;
                    dataTypeObject.push(o);
                }
            }

            var currentTableTypeCreate = createTableQuery(randomName + "_type", "field,type,datatype".split(","));
            console.log("[" + currentID + "] " + currentTableTypeCreate);

            return utils.queryPostgres(client, currentTableTypeCreate)
        }).then(dbres => {
            console.log("[" + currentID + "] Loading datatypes... (" + randomName +")");
            return utils.queryPostgres(client, insertDataTypeQuery(randomName + "_type", dataTypeObject));
        }).then(dbres => {
            console.log("[" + currentID + "] Loading data... (" + randomName +")");
            return utils.csvToPostgres(client, randomName, randomName);
        }).then(function () {
            console.log("[" + currentID + "] Finish! (" + endpoint + ")");
            resolve(randomName);
        }).catch(error => {
            reject(error);
        });
    });
}

standardResponseJSON = function (postgresArray) {
    //return new Promise((resolve, reject) => {
        var finalSelectHeader = [];
        for (var j in postgresArray[0]) {
            finalSelectHeader.push(j);
        }

        var resArray = [];
        for (var i = 0; i < postgresArray.length; i++) {
            var o = {};
            for (var j = 0; j < finalSelectHeader.length; j++) {
                o[finalSelectHeader[j]] = {};
                o[finalSelectHeader[j]]["type"] = "literal";
                o[finalSelectHeader[j]]["value"] = postgresArray[i][finalSelectHeader[j]];

                //XXX non così, meglio salvarlo nel db
                if (!isNaN(postgresArray[i][finalSelectHeader[j]])) {
                    o[finalSelectHeader[j]]["datatype"] = "http://www.w3.org/2001/XMLSchema#integer";
                }
                if (postgresArray[i][finalSelectHeader[j]].startsWith("Point(")) {
                    o[finalSelectHeader[j]]["datatype"] = "http://www.opengis.net/ont/geosparql#wktLiteral";
                }
            }
            resArray.push(o);
        }
        var replyObj = {};
        replyObj.head = {};
        replyObj.head.vars = finalSelectHeader;
        replyObj.results = {};
        replyObj.results.bindings = resArray;

        return replyObj;
    //});
}

joinAndResult = function (client, tableList, finalSelectHeader, finalGroupBy, finalCount, finalHaving, done) {
    return new Promise((resolve, reject) => {
        var commonVariable = "key"; //XXX

        var query = `SELECT ${finalSelectHeader}`

        if (finalCount !== undefined) query += `, COUNT(*) AS ${finalCount}`

        query += `
        FROM "${tableList[0]}"
        INNER JOIN "${tableList[1]}" ON "${tableList[1]}".${commonVariable} = "${tableList[0]}".${commonVariable}
        `

        var finalGroupByString = finalGroupBy.toString();
        if (finalGroupBy.length > 0) {
            query += `GROUP BY ${finalGroupByString}`;
        }

        if (finalHaving !== undefined) {
            query += `
            HAVING ${finalHaving}`;
        }

        console.log("[" + currentID + "] JOIN\n" + query);

        utils.queryPostgres(client, query).then(dbres => {
            resolve(dbres);
        }).catch(error => {
            reject(error);
        })
    });
}

exports.main = function (serviceQuery, sessionID, reply) {
    currentID = sessionID;
    console.log("[" + currentID + "] GOT A NEW QUERY TO DELVE\n" + serviceQuery);
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("[" + currentID + "] Error in connecting with Postgres" + err);
            reply(false);
            return;
        }

        var finalSelectHeader = [];
        var parsedQuery;
        var finalGroupBy = [];
        var finalCount;
        var finalHaving;

        try {
            parsedQuery = parser.parse(serviceQuery);
        } catch (e) {
            console.error(e);
            reply(false);
            return;
        }

        if (parsedQuery.type !== "query" || !checkQueryPattern(parsedQuery)) {
            console.error("[" + currentID + "] Not a valid pattern!");
            reply(false);
            return;
        }

        for (var i = 0; i < parsedQuery.variables.length; i++) {
            if (typeof parsedQuery.variables[i] === 'string') {
                finalSelectHeader.push(parsedQuery.variables[i].replace("?",""));
            } else if (parsedQuery.variables[i].expression.aggregation === "count") {
                finalCount = parsedQuery.variables[i].variable.replace("?","");
            }
        }
        if (parsedQuery.group !== undefined) {
            for (var i = 0; i < parsedQuery.group.length; i++) {
                finalGroupBy.push(parsedQuery.group[i].expression.replace("?",""));
            }
        }
        if (parsedQuery.having !== undefined) {
            parsedQuery.having[0].args
            finalHaving = parsedQuery.having[0].args[0].aggregation + "(" + parsedQuery.having[0].args[0].expression + ") "
                          + parsedQuery.having[0].operator + " " + parsedQuery.having[0].args[1].replace('"','').replace('"^^http://www.w3.org/2001/XMLSchema#integer','');
        }

        var c = 0;
        var allTable = [];
        for (var i = 0; i < parsedQuery.where.length; i++) {
            var e = parsedQuery.where[i].name;
            var q = generator.stringify(parsedQuery.where[i]);
            singleQueryRun(client, q, e).then(table => {
                allTable.push(table);

                /* if its my last call for this query */
                if (++c === parsedQuery.where.length) {
                    for (var j = 0; j < allTable.length; j++) {
                        fs.unlinkSync(allTable[j]);
                    }
                    return joinAndResult(client, allTable, finalSelectHeader, finalGroupBy, finalCount, finalHaving);
                }
            }).then(res => {
                if (res !== undefined) { //XXX
                    reply(standardResponseJSON(res));

                    var deleteQuery = 'DROP TABLE "' + allTable.toString().replace(/,/g,'","') + '"';
                    console.log("[" + currentID + "]" + deleteQuery);
                    utils.queryPostgres(client, deleteQuery).then(dbres => {
                        done();
                    }).catch(error => {
                        console.error(error);
                        reply(false);
                        return;
                    })
                }
            }).catch(error => {
                console.error("[" + currentID + "] " + error);
                reply(false);
                return;
            })
        }

    });
}
