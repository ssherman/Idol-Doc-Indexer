var aws = require ('aws-lib');
var events = require('events');
var sys = require('sys')
var http = require('http');
var fs = require('fs');
var knox = require('knox');
var url = require('url');
var path = require('path');
var querystring = require('querystring');
var SqsPoller = require('./aws_sqs_poller');

// read preferences json file from disk
var pref_file = fs.readFileSync('sqs_preferences.json', 'utf8');
var data = JSON.parse(pref_file);

var aws_key = data.aws_key;
var aws_secret_key = data.aws_secret;
var sqs_queue_path = data.sqs_queue_path;
var dir_to_save_docs_to = data.docs_dir;


var options = {
	'path': sqs_queue_path
};

var sqs_poller = new SqsPoller(aws_key, aws_secret_key, options, 1000, 30000, 5000);

// this event is called when there are new messages on the queue
sqs_poller.on('new_messages', function(message, sqs) {
    var message_id = message.MessageId;

    // the body is json
    var body = message.Body;
    var receipt_handle = message.ReceiptHandle;

    console.log("message_id: " + message_id + ", body: " + body);

    var json_data = JSON.parse(body);

    // the action will be either index or unindex
    var action = json_data.action;

    // ***** UNINDEX ****** //
    if ( json_data.action == "unindex" ) {
        unindex(json_data, receipt_handle, sqs);

    // ****** INDEX ****** //
    } else if ( json_data.action == "index" ) {
        index(json_data, receipt_handle, sqs);
    // ****** unknown message. lets just delete it so we don't keep getting it ****** //
    } else {
        sqs.call('DeleteMessage', {'ReceiptHandle':receipt_handle}, function(result) {
           console.log("delete result: " + result);
        });
    }

});

// this handles error cases like if sqs can not connect at all or the queue is invalid
sqs_poller.on('error', function(message, sqs) {
   console.log('Error: ' + message); 
});

// Unindex a document from autonomy. 
// json_data - data required to unindex an autonomy db
// receipt_handle - id required to delete/confirm a message from a sqs queue
function unindex(json_data, receipt_handle, sqs) {

    // unique autonomy id
    var reference = json_data.reference;

    // each customer has their own autonomy db
    var db = json_data.db;
    console.log("unindex reference: " + reference + ", db: " + db);

    var http_options = {
        host: 'localhost',
        port: 9001,
        path: '/DREDELETEREF?Docs=' + reference + '&DREDbName=' + db
    };

    // post to autonomy to unindex the document
    var unindex_request = http.get(http_options, function (res) {

        console.log("unindex response: " + res.statusCode);

        // delete the sqs message so the queue won't continuously read it
        sqs.call('DeleteMessage', {'ReceiptHandle':receipt_handle}, function(result) {
           console.log("delete result: " + result);
        });

   }).on('error', function (e) {
    console.log("unindex error: " + e.message);
   });
}

// index a document into autonomy. Only documents that can be cracked(pdf, word, xls, ppt).
// go through this method of indexing.
//
// Basically what happens is we download the document from an s3 bucket, write it to the
// filesystem, then create an autonomy index request pointing to the location on the
// filesystem.
// json_data = data required to index a document
function index(json_data, receipt_handle, sqs) {

    var key = json_data.s3_aws_key;
    var secret = json_data.s3_aws_secret;
    var bucket = json_data.s3_bucket;
    var file_id = json_data.file_id;
    var file_name = json_data.file_name;
    var autonomy_db_name = json_data.autonomy_db_name;
    var stubidx = json_data.stubidx;

    // create the s3 client
    var client = knox.createClient({
        key: key,
        secret: secret,
        bucket: bucket
    });

    console.log("submitting the s3 request to download the document");
    var s3_path = file_id + '/' + querystring.escape(file_name);
    
    // request the s3 document
    client.get(s3_path).on('response', function (s3_res) {
        console.log("downloading " + s3_path + " from s3...");
        console.log(s3_res.statusCode);
        console.log(s3_res.headers);

        // create a directory with the same name as the db on the autonomy server
        var path_to_file = dir_to_save_docs_to + autonomy_db_name + "_" + file_name;

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
                'DREDBNAME': autonomy_db_name,
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

                    // delete the message... we got a response back from autonomy
                    // TODO: parse the autonomy message and see if there were any
                    // autonomy errors then do what????
                    sqs.call('DeleteMessage', {'ReceiptHandle':receipt_handle}, function(result) {
                        console.log("delete result: " + result);
                    }); 
                });
                res.on('error', function (e) {
                    console.log('problem with request: ' + e.message);
                });
            });

            post_req.on('error', function(error) {
              console.log('error trying to make autonomy request ' + error);
            });


            // post the data
            post_req.write(idol_data);
            post_req.end();
        });
    }).end();

}
sqs_poller.start();

