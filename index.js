// dependencies
var async = require('async');
var AWS = require('aws-sdk');
var gm = require('gm')
            .subClass({ imageMagick: true }); // Enable ImageMagick integration.
var util = require('util');
var _ = require('underscore');

// get reference to S3 client
var s3 = new AWS.S3();
var sqs = new AWS.SQS();
var queueUrl = "https://sqs.us-east-1.amazonaws.com/760079153816/passitdown_notifications";

exports.handler = function(event, context) {
	// Read options from the event.
	console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
	var srcBucket = event.Records[0].s3.bucket.name;
	var srcKey    = event.Records[0].s3.object.key;
  var dstBucket = srcBucket.replace("-inbox", "");
  var dstKey    = srcKey;
  var imageType = "";
  var width = 0;
  var height = 0;

	// Infer the image type.
	var typeMatch = srcKey.match(/\.([^.]*)$/);
	if (!typeMatch) {
		console.error('unable to infer image type for key ' + srcKey);
		return;
	}
	imageType = typeMatch[1].toLowerCase();
	if (imageType != "jpg" && imageType != "png") {
		console.log('skipping non-image ' + srcKey);
		return;
	}

	// Download the image from S3, transform, and upload to a different S3 bucket.
	async.waterfall([
		function download(next) {
			// Download the image from S3 into a buffer.
			s3.getObject({
					Bucket: srcBucket,
					Key: srcKey
				},
        function (err, response) {
				  next(err, response);
        });
			},
		function transform(response, next) {
			gm(response.Body).orientation(function(err, value) {
        if (value==='Undefined') {
            console.log("image hasn't any exif orientation data");
            next(null, response.ContentType, response.Body);
        } else {
            console.log("auto orienting image with exif data", value);
				    // Transform the image buffer in memory.
				    this.autoOrient().toBuffer(imageType, function(err, buffer) {
						if (err) {
							next(err);
						} else {
							next(null, response.ContentType, buffer);
						}
					});
        }
			});
		},
    function appendDimensions(contentType, data, next) {
      gm(data).size(function(err, size) {
        if (err) {
          next(err);
        } else {
          width = size.width;
          height = size.height;
          dstKey = srcKey;//.replace("." + imageType, "_" + width + "x" + height + "." + imageType)
          next(null, contentType, data);
        }
      });
    },
		function upload(contentType, data, next) {
			// Stream the transformed image to a different S3 bucket.
			w = width.toString();
      h = height.toString();
      console.log('Width: ' + w + '; height: ' + h);
      s3.putObject({
				Bucket: dstBucket,
				Key: dstKey,
				Body: data,
				ContentType: contentType,
        ACL: 'public-read',
        Metadata: {
          width: w,
          height: h
        }
			}, function(err, data) {
        next(err, data);
      });
		},
    function writeToQueue(data, next) {
      var metadata = {
        originalFilename: srcKey,
        width: width,
        height: height,
        url: "https://s3.amazonaws.com/" + dstBucket + "/" + dstKey
      };
      var json = JSON.stringify(metadata);
      console.log("Sending message to SQS: " + json);
      sqs.sendMessage({
        MessageBody: json,
        QueueUrl: queueUrl
      }, function(err, data) {
        if (err) {
          console.error(err);
          next(err);
        } else {
          console.error("SQS write successful");
          next(null);
        }
      });
    },
    function deleteOriginalFile(next) {
      console.log("Deleting original file...");
      s3.deleteObject({
        Bucket: srcBucket,
        Key: srcKey
      }, function(err, data) {
        next(err);
      });
    }
	], function (err) {
		if (err) {
			console.error(
				'Unable to process ' + srcBucket + '/' + srcKey +
				' and upload to ' + dstBucket + '/' + dstKey +
				' due to an error: ' + err
			);
		} else {
			console.log(
				'Successfully processed ' + srcBucket + '/' + srcKey +
				' and uploaded to ' + dstBucket + '/' + dstKey
			);
		}

		context.done();
	});
};
