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
        params += "\"" + headArray[i] + "\" varchar,";
    }
    params = params.slice(0, -1);

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${params});`;
}

/* field, type, datatype
   test, literal, http://... */
insertDataTypeQuery = function (tableName, dataTypeObject) {
    var query = "";
    for (var i = 0; i < dataTypeObject.length; i++) {
        query += `INSERT INTO "${tableName}" (field, database, type, datatype)
          VALUES ('${dataTypeObject[i].field}', '${dataTypeObject[i].database}', '${dataTypeObject[i].type}', '${dataTypeObject[i].datatype}'); `;
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
                        if (dataVector[i][header[j]] !== undefined) {
                            line.push(dataVector[i][header[j]].value);
                        } else {
                            line.push("null");
                        }
                    }
                    var quotedAndCommaSeparated = '"' + line.join('","') + '"';
                    fs.appendFileSync(randomName, quotedAndCommaSeparated + "\n");
                }

                var vars = sparqlResult.body.head.vars;
                for (var k in vars) {
                    var o = {};
                    o.field = vars[k];
                    o.database = randomName;
                    o.type = utils.findElementJsonArrayByKey(dataVector, vars[k]).type;
                    o.datatype = utils.findElementJsonArrayByKey(dataVector, vars[k]).datatype;
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

            var currentTableTypeCreate = createTableQuery("type", "field,database,type,datatype".split(","));
            console.log("[" + currentID + "] " + currentTableTypeCreate);

            return utils.queryPostgres(client, currentTableTypeCreate)
        }).then(dbres => {
            console.log("[" + currentID + "] Loading datatypes... (" + randomName +")");
            return utils.queryPostgres(client, insertDataTypeQuery("type", dataTypeObject));
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

standardResponseJSON = function (client, tableList, postgresArray) {
    return new Promise((resolve, reject) => {
        var finalSelectHeader = [];
        for (var j in postgresArray[0]) {
            finalSelectHeader.push(j);
        }

        tableList.push(currentID);
        var dbList = "'" + tableList.join("','") + "'";
        var getTypesQuery = `SELECT * FROM type WHERE database IN (${dbList})`;
        console.log("[" + currentID + "] " + getTypesQuery);
        utils.queryPostgres(client, getTypesQuery).then(dbres => {
            var resArray = [];
            for (var i = 0; i < postgresArray.length; i++) {
                var o = {};
                for (var j = 0; j < finalSelectHeader.length; j++) {
                    if (postgresArray[i][finalSelectHeader[j]] === "null") continue;

                    o[finalSelectHeader[j]] = {};
                    o[finalSelectHeader[j]]["datatype"] = utils.findElementJsonArray(dbres, "field", finalSelectHeader[j])["datatype"];
                    o[finalSelectHeader[j]]["type"] = utils.findElementJsonArray(dbres, "field", finalSelectHeader[j])["type"];
                    o[finalSelectHeader[j]]["value"] = postgresArray[i][finalSelectHeader[j]];

                    if (o[finalSelectHeader[j]]["datatype"] === "undefined") delete o[finalSelectHeader[j]]["datatype"];
                }
                resArray.push(o);
            }
            var replyObj = {};
            replyObj.head = {};
            replyObj.head.vars = finalSelectHeader;
            replyObj.results = {};
            replyObj.results.bindings = resArray;

            resolve(replyObj);
        }).catch(error => {
            reject(error);
        });
    });
}

joinAndResult = function (client, tableList, finalSelectHeader, finalGroupBy, finalCount, finalHaving, done) {
    return new Promise((resolve, reject) => {
        var commonVariable = "key"; //XXX

        var finalSelectHeaderString = '"' + finalSelectHeader.join('","') + '"';
        var query = `SELECT ${finalSelectHeaderString}`

        if (finalCount !== undefined) query += `, COUNT(*) AS ${finalCount}`

        query += `
        FROM "${tableList[0]}"
        INNER JOIN "${tableList[1]}" ON "${tableList[1]}".${commonVariable} = "${tableList[0]}".${commonVariable}
        `

        var finalGroupByString = '"' + finalGroupBy.join('","') + '"';
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
    console.log("[" + currentID + "] OPEN POOL CONNECTION");
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("[" + currentID + "] Error in connecting with Postgres\n" + err);
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
            console.error("[" + currentID + "] Invalid query!\n" + e);
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
                var o = {};
                o.field = finalCount;
                o.database = currentID;
                o.type = "literal";
                o.datatype = "http://www.w3.org/2001/XMLSchema#integer";
                dataTypeObject.push(o);
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

                    joinAndResult(client, allTable, finalSelectHeader, finalGroupBy, finalCount, finalHaving).then(res => {
                        return standardResponseJSON(client, allTable, res);
                    }).then(res => {
                        reply(res);

                        allTable.splice(allTable.indexOf(currentID), 1);
                        var deleteQuery = 'DROP TABLE "' + allTable.toString().replace(/,/g,'","') + '"';
                        console.log("[" + currentID + "] " + deleteQuery);

                        return utils.queryPostgres(client, deleteQuery)
                    }).then(dbres => {
                        console.log("[" + currentID + "] CLOSE POOL CONNECTION");
                        done();
                    }).catch(err => {
                        console.error("[" + currentID + "] " + err);
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
