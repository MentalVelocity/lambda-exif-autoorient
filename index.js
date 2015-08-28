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
var xcoder = new AWS.ElasticTranscoder();
var devQueueUrl = "https://sqs.us-east-1.amazonaws.com/760079153816/passitdown_notifications_development";
var prodQueueUrl = "https://sqs.us-east-1.amazonaws.com/760079153816/passitdown_notifications_production";
var stagingQueueUrl = "https://sqs.us-east-1.amazonaws.com/760079153816/passitdown_notifications_staging";
var devVideoPipelineId = "1437962088972-25fnit";
var prodVideoPipelineId = "1437962171995-pmsi72";
var stagingVideoPipelineId = "1440784627448-wbk7dq";
//var videoPresetId = "1351620000001-100070"; // "web" preset
var videoPresetId = "1439263044305-x6bcxl"; // custom web preset
var audioPresetId = "1351620000001-300040"; // mp3 128k preset

exports.handler = function(event, context) {
  // Read options from the event.
  console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
  var srcBucket = event.Records[0].s3.bucket.name;
  var srcKey    = event.Records[0].s3.object.key;
  var environment = srcBucket.indexOf("-production") > -1 ? "production" : (srcBucket.indexOf("-staging") > -1 ? "staging" : "development");
  var queueUrl = environment == "production" ? prodQueueUrl : (environment == "staging" ? stagingQueueUrl : devQueueUrl);
  var videoPipelineId = environment == "production" ? prodVideoPipelineId : (environment == "staging" ? stagingVideoPipelineId : evVideoPipelineId);
  var audioPipelineId = videoPipelineId
  var dstBucket = srcBucket.replace("-inbox", "");
  var dstKey    = srcKey;
  var archiveBucket = srcBucket.replace("-inbox", "-archive");
  var archiveKey    = srcKey;
  var extension = "";
  var width = 0;
  var height = 0;
  var fileType = "other";
  var imageType = "";

  // Infer the type from the extension.
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.error('unable to infer file type for key ' + srcKey);
    return;
  }
  extension = typeMatch[1].toLowerCase();
  switch(extension) {
    case "jpg":
    case "jpeg":
      imageType = "jpg";
      fileType = "image";
      break;
    case "png":
      imageType = "png";
      fileType = 'image';
      break;
    case "m4v":
    case "mov":
    case "mp4":
      fileType = 'video';
      break;
    case "mp3":
    case 'm4a':
      fileType = 'audio';
      break;
    default:
      fileType = 'other';
      break;
  }


  if (fileType == 'other') {
    console.log('unsupported file type ' + srcKey);
    return;
  } else {
    console.log("Filetype: " + fileType);
  }

  if (fileType == 'image') {
    // Download the image from S3, transform, and upload to a different S3 bucket.
    console.log("filetype identified as image...");

    var source_attachable_type = srcKey.indexOf("-story") > -1 ? "story" : "user";
    console.log("Source attachment belongs to: " + source_attachable_type);
    if (source_attachable_type == "story") {
      async.waterfall([
        function download(next) {
        // Download the image from S3 into a buffer.
        s3.getObject({
            Bucket: srcBucket,
            Key: srcKey
          },
          function (err, response) {
            if (err) {
              console.log("failed to fetch s3 object " + srcKey + ": " + err);
              next(err);
            } else {
              console.log("Retrieved s3 object " + srcKey);
              next(null, response);
            }
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
          context.fail();
        } else {
          console.log(
            'Successfully processed ' + srcBucket + '/' + srcKey +
            ' and uploaded to ' + dstBucket + '/' + dstKey
          );
        }
        context.succeed();
      });
    } else {
      // *** PROFILE IMAGES ***
      async.waterfall([
        function download(next) {
        // Download the image from S3 into a buffer.
        s3.getObject({
            Bucket: srcBucket,
            Key: srcKey
          },
          function (err, response) {
            if (err) {
              console.log("failed to fetch s3 object " + srcKey + ": " + err);
              next(err);
            } else {
              console.log("Retrieved s3 object " + srcKey);
              next(null, response);
            }
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
                this.autoOrient()
                    .resize(240, 240, "^")
                    .gravity("Center")
                    .extent(240, 240)
                    .toBuffer(imageType, function(err, buffer) {
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
          context.fail();
        } else {
          console.log(
            'Successfully processed ' + srcBucket + '/' + srcKey +
            ' and uploaded to ' + dstBucket + '/' + dstKey
          );
        }
        context.succeed();
      });
    }
  }

  if (fileType == 'audio') {
    dstKey = dstKey.replace("." + extension, ".mp3");
    console.log("Filetype identified as audio...");
    var xcoder_params = {
      Input: {
        Key: srcKey
      },
      PipelineId: audioPipelineId,
      Outputs: [
        {
          Key: dstKey,
          PresetId: audioPresetId
        }
      ]
    };
    console.log("audio job params: " + JSON.stringify(xcoder_params))

    async.waterfall([
      function getFile(next) {
        // Download the image from S3 into a buffer.
        s3.getObject({
          Bucket: srcBucket,
          Key: srcKey
        },
        function (err, response) {
          if (err) {
            next(err);
          } else {
            next(null, response.ContentType, response.Body);
          }
        });
      },
      function writeFileToArchive(contentType, data, next) {
        s3.putObject({
          Bucket: archiveBucket,
          Key: archiveKey,
          Body: data,
          ContentType: contentType,
          ACL: 'public-read'
        }, function(err, data) {
          next(err);
        });
      },
      function createTranscoderJob(next) {
        xcoder.createJob(xcoder_params, function(err, data) {
          if (err) {
            console.error('Error creating transcoder job for file ' + srcKey + ": " + err);
            next(err);
          }
          else {
            console.log('Successfully created transcoder job for file ' + srcKey);
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
          ' due to an error: ' + err
        );
        context.fail();
      } else {
        console.log(
          'Successfully processed ' + srcBucket + '/' + srcKey
        );
      }
      context.succeed();
    });
  }

  if (fileType == 'video') {
    dstKey = dstKey.replace("." + extension, ".m4v");
    console.log("Filetype identified as video...");
    var xcoder_params = {
      Input: {
        Key: srcKey
      },
      PipelineId: videoPipelineId,
      Outputs: [
        {
          ThumbnailPattern: dstKey + "-{count}",
          Key: dstKey,
          PresetId: videoPresetId
        }
      ]
    };
    console.log("video job params: " + JSON.stringify(xcoder_params))

    async.waterfall([
      function getFile(next) {
        // Download the image from S3 into a buffer.
        s3.getObject({
          Bucket: srcBucket,
          Key: srcKey
        },
        function (err, response) {
          if (err) {
            next(err);
          } else {
            next(null, response.ContentType, response.Body);
          }
        });
      },
      function writeFileToArchive(contentType, data, next) {
        s3.putObject({
          Bucket: archiveBucket,
          Key: archiveKey,
          Body: data,
          ContentType: contentType,
          ACL: 'public-read'
        }, function(err, data) {
          next(err);
        });
      },
      function createTranscoderJob(next) {
        xcoder.createJob(xcoder_params, function(err, data) {
          if (err) {
            console.error('Error creating transcoder job for file ' + srcKey + ": " + err);
            next(err);
          }
          else {
            console.log('Successfully created transcoder job for file ' + srcKey);
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
          ' due to an error: ' + err
        );
        context.fail();
      } else {
        console.log(
          'Successfully processed ' + srcBucket + '/' + srcKey
        );
      }
      context.succeed();
    });
  }
};
