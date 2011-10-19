var http = require('http');
var knox = require('knox');
var url = require('url');
var fs = require('fs');
var path = require('path');
var querystring = require('querystring');

http.createServer(function (req, res) {
    if (req.url === '/favicon.ico') {
        res.writeHead(200, {
            'Content-Type': 'image/x-icon'
        });
        res.end();
        console.log('favicon requested');
        return;
    }
    res.writeHead(200, {
        'Content-Type': 'text/plain'
    });

    var post_data = "";
    if (req.method == 'POST') {
        req.addListener('data', function (chunk) {
            post_data += chunk;
        }).addListener('end', function () {
            res.end('thanks');
            post_data = querystring.parse(post_data);
            var data_path = path.resolve('./data');
            var key = post_data.key;
            var secret = post_data.secret;
            var bucket = post_data.bucket;
            var dir = post_data.dir;
            var filename = post_data.filename;
            var stubidx = post_data.stubidx;
            var db_name = post_data.db_name;

            // create the s3 client
            var client = knox.createClient({
                key: key,
                secret: secret,
                bucket: bucket
            });

            console.log("submitting the s3 request to download the document");
            // request the s3 document
            client.get(dir + "/" + filename).on('response', function (s3_res) {
                var outstream = fs.createWriteStream("./data/" + filename);
                console.log(s3_res.statusCode);
                console.log(s3_res.headers);

                // stream the document to disk chunk by chunk
                s3_res.on('data', function (chunk) {
                    outstream.write(chunk);
                });

                // the file has been saved! now let's build the autonomy request
                s3_res.on('end', function () {
                    console.log("Submitting the data to autonomy filesystemfetch");
                    outstream.end();
                    var path_to_file = data_path + "/" + filename;
                    var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + path_to_file + "\"/></autn:document></autn:envelope></autn:import>";
                    console.log(xml);

                    var post_data = querystring.stringify({
                        'Data': stubidx,
                        'DREDBNAME': db_name,
                        'EnvelopeXML': xml,
                        'jobname': 'ImportEnvelopeJob',
                        'EnvelopeImportFailOnImport': 'never'
                    });

                    var http_options = {
                        host: 'localhost',
                        port: 7000,
                        path: '/',
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Content-Length': post_data.length
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
                });
            });
        });
    }
}).listen(1337);
console.log('Server running at http://127.0.0.1:1337/');
