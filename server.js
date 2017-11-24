var express      = require('express');
var morgan       = require('morgan');
var cors         = require('cors');
var balrog       = require('./balrog');

var app = express();
app.use(morgan('common'));

app.get('/sparql', cors(), function (request, response) {
    balrog.main(request.query.query, function (result) {
        response.send(result);
    });
});

var server = app.listen(4242, function() {
    var host = server.address().address;
    var port = server.address().port;
    console.log('Server listening at http://%s:%s', host, port);
});
