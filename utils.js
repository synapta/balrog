var fs = require('fs');
var copyFrom = require('pg-copy-streams').from;

exports.queryPostgres = function (client, query) {
    return new Promise((resolve, reject) => {
        client.query(query, function (err, result) {
            if (err) {
                return reject(err);
            }
            return resolve(result.rows);
        });
    });
}

exports.csvToPostgres = function (client, tableName, fileName) {
    return new Promise((resolve, reject) => {
        var stream = client.query(copyFrom('COPY "' + tableName +'" FROM STDIN WITH CSV HEADER ESCAPE\'\\\''));
        var fileStream = fs.createReadStream("./" + fileName);
        fileStream.on('error', function(error) { reject('Error:', error.message) });
        fileStream.pipe(stream)
            .on('finish', resolve())
            .on('error', function(error) { reject('Error:', error.message) });
    });
}
