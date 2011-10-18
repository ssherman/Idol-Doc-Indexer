var http = require('http');
var knox = require('knox');
var url  = require('url');
var fs = require('fs');
var path = require('path');
var querystring = require('querystring');

http.createServer(function (req, res) {
  if (req.url === '/favicon.ico') {
    res.writeHead(200, {'Content-Type': 'image/x-icon'} );
    res.end();
    console.log('favicon requested');
    return;
  }
  res.writeHead(200, {'Content-Type': 'text/plain'});
  var data_path = path.resolve('./data');
  var parsed_url = url.parse(req.url, true);
  var key = parsed_url.query.key;
  var secret = parsed_url.query.secret;
  var bucket = parsed_url.query.bucket;
  var dir = parsed_url.query.dir;
  var filename = parsed_url.query.filename;
  var stubidx = parsed_url.query.stubidx;
  var db_name = parsed_url.query.db_name;

 	var client = knox.createClient({key: key, secret: secret, bucket: bucket});
 	
	client.get(dir + "/" + filename).on('response', function(s3_res){
		var outstream = fs.createWriteStream("./data/" + filename);
  	console.log(s3_res.statusCode);
  	console.log(s3_res.headers);
  	
  	s3_res.on('data', function(chunk){
    	outstream.write(chunk);
  	});

  	s3_res.on('end', function(){
  		outstream.end();
  		var path_to_file = data_path + "/" + filename;
			var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + path_to_file + "\"/></autn:document></autn:envelope></autn:import>";
			console.log(xml);

		  var post_data = querystring.stringify({
    	  'Data' : stubidx,
     	  'DREDBNAME': db_name,
     	  'EnvelopeXML': xml,
        'jobname' : 'ImportEnvelopeJob',
        'EnvelopeImportFailOnImport' : 'never'
  		});

			var http_options = {
			  host: 'localhost',
			  port: 7000,
			  path: '/upload',
			  method: 'POST',
      	headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': post_data.length
      	}
			};

		  // Set up the request
		  var post_req = http.request(http_options, function(res) {
		      res.setEncoding('utf8');
		      res.on('data', function (chunk) {
		          console.log('Response: ' + chunk);
		      });
					res.on('error', function(e) {
  					console.log('problem with request: ' + e.message);
					});
		  });

  	});
	}).end();

  res.end(parsed_url.query.name + '\n');
}).listen(1337, "127.0.0.1");
console.log('Server running at http://127.0.0.1:1337/');