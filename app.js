var connect = require('connect');
var knox = require('knox');
var http = require('http');
var url = require('url');
var fs = require('fs');
var path = require('path');
var querystring = require('querystring');

var app = connect.createServer();
app.use(connect.favicon());
app.use(connect.logger());
app.use(connect.bodyParser());
app.use(connect.errorHandler({
    dumpExceptions: true,
    showStack: true,
    showMessage: true
}));

var dir_to_save_docs_to = "/mnt/Autonomy/filesystemfetch_queue/";

app.use(connect.router(function (app) {
    app.get('/', function (req, res, next) {
        res.writeHead(200, {
            'Content-Type': 'text/plain'
        });
        res.end('Welcome to Idol-Doc-Indexer. API docs should go here or something');
    });
    app.post('/index', function (req, res, next) {
        var key = req.body.key;
        var secret = req.body.secret;
        var bucket = req.body.bucket;
        var file_s3_key = encodeURIComponent(req.body.file_s3_key);
        var filename = req.body.filename;
        var stubidx = req.body.stubidx;
        var db_name = req.body.db_name;

        // create the s3 client
        var client = knox.createClient({
            key: key,
            secret: secret,
            bucket: bucket
        });

        console.log("submitting the s3 request to download the document");
        // request the s3 document
        client.get(file_s3_key).on('response', function (s3_res) {
            console.log("downloading " + file_s3_key + " from s3...");
            console.log(s3_res.statusCode);
            console.log(s3_res.headers);

            // create a directory with the same name as the db on the autonomy server
            var path_to_file = dir_to_save_docs_to + db_name + "_" + filename;

            // stream the document to disk chunk by chunk
            var outstream = fs.createWriteStream(path_to_file);
            s3_res.on('data', function (chunk) {
                outstream.write(chunk);
            });

            // the file has been saved! now let's build the autonomy request
            s3_res.on('end', function () {
                console.log("Submitting the data to autonomy filesystemfetch");
                outstream.end();
                var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + path_to_file + "\" deleteoriginal=\"true\" /></autn:document></autn:envelope></autn:import>";
                console.log(xml);

                var idol_data = querystring.stringify({
                    'Data': stubidx,
                    'DREDBNAME': db_name,
                    'EnvelopeXML': xml,
                    'jobname': 'ImportEnvelopeJob',
                    'EnvelopeImportFailOnImport': 'never'
                });

                var http_options = {
                    host: 'localhost',
                    port: 7000,
                    path: '/action=ImportEnvelope',
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Content-Length': idol_data.length
                    }
                };

                // post to autonomy to index the document
                var post_req = http.request(http_options, function (res) {
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {
                        console.log('Response: ' + chunk);
                    });
                    res.on('error', function (e) {
                        console.log('problem with request: ' + e.message);
                    });
                });

                // post the data
                post_req.write(idol_data);
                post_req.end();
            });
        }).end();
        res.writeHead(200, {
            'Content-Type': 'text/plain'
        });
        res.end('indexing job successfully submitted');
    });
    app.get('/unindex/:database/:drereference', function (req, res, next) {

        var http_options = {
            host: 'localhost',
            port: 9001,
            path: '/DREDELETEREF?Docs=' + req.params.drereference + '&DREDbName=' + req.params.database
        };

        // post to autonomy to index the document
        var unindex_request = http.get(http_options, function (res) {
            console.log("unindex response: " + res.statusCode);
        }).on('error', function (e) {
            console.log("unindex error: " + e.message);
        });

        res.writeHead(200, {
            'Content-Type': 'text/plain'
        });
        res.end('unindex request successfully sent.');
    });
}));
app.listen(1337);
console.log('Server running at http://127.0.0.1:1337/');
