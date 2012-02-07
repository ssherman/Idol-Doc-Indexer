var aws = require ('aws-lib');
var events = require('events');
var sys = require('sys');


function AwsSqsPoller(aws_key, aws_private_key, sqs_options, initial_timeout, max_timeout, timeout_increment) {
    this.aws_key = aws_key;
    this.aws_private_key = aws_private_key;
    this.sqs_options = sqs_options;
    this.sqs = aws.createSQSClient(this.aws_key, this.aws_private_key, this.sqs_options);
    this.initial_timeout = initial_timeout;
    this.max_timeout = max_timeout;
    this.timeout_increment = timeout_increment;
    self = this;
}

AwsSqsPoller.prototype = new events.EventEmitter();
AwsSqsPoller.prototype.start = function start() {
    this._timeout = setTimeout(this.getMessages, self.initial_timeout, self.initial_timeout);
};

AwsSqsPoller.prototype._restart = function restart(timeout) {
   this._timeout = setTimeout(this.getMessages, timeout, timeout); 
};

AwsSqsPoller.prototype.getMessages = function getMessages(timeout) {
    self.sqs.call('ReceiveMessage', {'MaxNumberOfMessages': 10}, function(result) {

        if ( result.Error ) {
            self.emit('error', result.Error.Message, self.sqs);
            return true;
        }

        var messages = result.ReceiveMessageResult.Message;

        if ( messages != "undefined" && messages != null && !Array.isArray(messages) ) {
            messages = [messages];
        }

        // sqs is weird. sometimes it returns undefined and then sometimes it returns
        // an object with a length of undefined
        if ( messages && typeof messages.length != "undefined") {

            for(i = 0; i < messages.length; i++) {
                self.emit('new_messages', messages[i], self.sqs);
            }
            console.log("\nincreasing poll frequency to " + self.initial_timeout + " second\n");

            // execute the timeout again with the same parameters
            self._restart(self.initial_timeout);
        } else {

            // since there's no messages let's increase timeout by 5 seconds unless
            // max_timeout reached
            var new_timeout = timeout + self.timeout_increment;
            if ( new_timeout >= self.max_timeout ) {
                new_timeout = self.max_timeout;
            }
            console.log("\ndelaying poll to be " + new_timeout + " seconds\n");
            self._restart(new_timeout);
        }
    });
};

module.exports = AwsSqsPoller;
