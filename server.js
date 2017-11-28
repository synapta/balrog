var express      = require('express');
var morgan       = require('morgan');
var cors         = require('cors');
var balrog       = require('./balrog');
var uuid         = require('node-uuid');

var app = express();
app.use(morgan('common'));

app.get('/sparql', cors(), function (request, response) {
    var requestID = uuid.v4();
    balrog.main(request.query.query, requestID, function (result) {
        if (!result) {
            response.status(500).send('Something broke!');
            console.log("55")
        } else {
            response.send(result);
        }
    });
});

var server = app.listen(4242, function() {
    var host = server.address().address;
    var port = server.address().port;
    console.log('Server listening at http://%s:%s', host, port);
});
