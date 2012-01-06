var sys = require('util');
var stomp = require('stomp');
var http = require('http');
var fs = require('fs');
var knox = require('knox');
var url = require('url');
var path = require('path');
var querystring = require('querystring');

// read preferences json file from disk
var pref_file = fs.readFileSync('preferences.json', 'utf8');
var data = JSON.parse(pref_file);
var pubsub_endpoint = data.pubsub_endpoint;
var pubsub_port = data.pubsub_port;

var dir_to_save_docs_to = data.docs_dir;//"/mnt/Autonomy/filesystemfetch_queue/";

var stomp_args = {
    port: pubsub_port,
    host: pubsub_endpoint,
    debug: true
};

var client = new stomp.Stomp(stomp_args);

var headers = {
    destination: '/queue/spaces_autonomy',
    ack: 'client-individual'
};

var messages = 0;

client.connect();

client.on('connected', function() {
    client.subscribe(headers);
    console.log('Connected');
});

client.on('message', function(message) {
    //console.log("HEADERS: " + sys.inspect(message.headers));
    console.log("BODY: " + message.body);
    console.log("Got message: " + message.headers['message-id']);

    var data = JSON.parse(message.body);
    var reference = data.reference;
    var action = data.action;
    var db = data.db;

    // unindex autonomy docs
    if ( action == "unindex" ) {
        console.log("unindex");
        console.log("reference: " + reference + ", db: " + db);

        var http_options = {
            host: 'idolrdev.ngenplatform.com',
            port: 9001,
            path: '/DREDELETEREF?Docs=' + reference + '&DREDbName=' + db
        };

        // post to autonomy to index the document
        var unindex_request = http.get(http_options, function (res) {
            client.ack(message.headers['message-id']);
            messages++;
            console.log("unindex response: " + res.statusCode);
        }).on('error', function (e) {
            console.log("unindex error: " + e.message);
        });

    // index large docs. We first save from s3 to the local filesystem, then index with autonomy    
    } else if ( action == "index" ) {
        console.log("action: index");
        var key = data.key;
        var secret = data.secret;
        var bucket = data.bucket;
        var file_s3_key = encodeURIComponent(data.file_s3_key);
        var filename = data.filename;
        var stubidx = data.stubidx;
        var db_name = data.db;


        // create the s3 client
        var s3_client = knox.createClient({
            key: key,
            secret: secret,
            bucket: bucket
        });

        console.log("submitting the s3 request to download the document");
        // request the s3 document
        s3_client.get(file_s3_key).on('response', function (s3_res) {
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
                console.log("Submitting the data to autonomy filesystemfetch for " + message.headers['message-id']);
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
                    host: 'idolrdev.ngenplatform.com',
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
                        console.log('---- SHANEWTF for message: ' + message.headers['message-id'] +  ': Response: ' + chunk);
                        client.ack(message.headers['message-id']);
                        messages++;
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
    }

});

client.on('error', function(error_frame) {
    console.log(error_frame.body);
    client.disconnect();
});

process.on('SIGINT', function() {
    console.log('\nConsumed ' + messages + ' messages');
    client.disconnect();
});