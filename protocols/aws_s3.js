const maxFileLength = 5368709120;
const maxTrials = 3;
const minPartSize = 5242880;
let path = require('path');
let S3 = require('aws-sdk/clients/s3');
let mime = require('mime-types');
let base = require("../base");
let qs = require("querystring");
let restores = {};

module.exports = class extends base {
    static parameters = {
        access_key: {secret: true},
        secret: {secret: true},
        bucket: {text: true},
        region: {text: true},
        polling: {boolean: true},
        polling_interval: {number: true}
    };
    constructor(params, logger) {
        super(params, logger, "aws_s3");
        this.on_file_restore = () => {};
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'aws_s3', accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
    }
    update_settings(params) {
        params.parallel_parsers = 1;
        this.bucket = params.bucket;
        if (this.id() !== this.constructor.generate_id(params)) this.S3 = new S3({accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
        super.update_settings(params);
    }
    read(filename, params = {}) {
        return this.restore(filename, params).then(() => this.queue.run(() => this.S3.getObject({Bucket: params.origin_bucket || params.bucket || this.bucket, Key: filename}).promise().then(data => this.constructor.get_data(data.Body, params.encoding))));
    }
    stat(filename, params = {}) {
        return this.queue.run(() => this.S3.headObject({Bucket: params.origin_bucket || params.bucket || this.bucket, Key: filename}).promise()).then(data => {
            let restore = data.Restore?.match(/ongoing-request="(.+?)"(?:, expiry-date="(.+?)")?/)
            return {
                size: data.ContentLength,
                mtime: data.LastModified,
                storage_class: data.StorageClass,
                needs_restore: (data.StorageClass === 'GLACIER' || data.StorageClass === 'DEEP_ARCHIVE') && (!restore || restore?.[1] === 'true'),
                restoring: restore?.[1] === 'true',
                isDirectory: () => filename.endsWith('/')
            }
        });
    }
    createReadStream(source, params = {}) {
        return this.restore(source, params)
            .then(() => {
                let range;
                if (params.start && params.end) range = `bytes=${params.start}-${params.end}`;
                return this.queue.run(() => this.S3.getObject({Bucket: params.origin_bucket || params.bucket || this.bucket, Key: source, Range: range}).createReadStream())
            });
    }
    mkdir(dir, params = {}) {
        return this.queue.run(() => this.S3.createBucket({Bucket: params.origin_bucket || params.bucket || this.bucket}).promise())
    }
    write(target, contents = '', params = {}) {
        return this.queue.run(() => this.S3.putObject({Bucket: params.origin_bucket || params.bucket || this.bucket, Key: target, Body: contents}).promise())
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) return this.local_copy(source, target, size, params);
        return this.queue.run(() => {
            return new Promise((resolve, reject) => {
                let partSize = params.partSize || 50 * 1024 * 1024;
                while (size / partSize > 10000) partSize *= 2;
                let options = params.options || {partSize: partSize, queueSize: params.concurrency || 8};
                let result = this.S3.upload({Bucket: params.bucket || this.bucket, Key: target, Body: streams.passThrough, ContentType: mime.lookup(source), StorageClass: params.storage_class, Tagging: qs.stringify(params.tags)}, options, (err,data) => {
                    if (err) reject(err);
                    else resolve();
                });
                streams.readStream.pipe(streams.passThrough);
                streams.readStream.on('error', err => reject(err));
                streams.passThrough.on('error', err => reject(err));
                if (params.publish) {
                    let percentage = 0;
                    result.on('httpUploadProgress', event => {
                        let tmp = Math.round(event.loaded * 100 / (event.total || size));
                        if (percentage != tmp) {
                            percentage = tmp;
                            params.publish({
                                current: event.loaded,
                                total: event.total || size,
                                percentage: percentage
                            });
                        }
                    });
                }
            })
            .then(() => {
                if (params.make_public) return this.S3.putObjectAcl({Bucket: this.bucket, ACL: "public-read", Key: target}).promise();
            })
        })
    }
    remove(target) {
        return this.queue.run(() => this.S3.deleteObject({Bucket: this.bucket, Key: target}).promise());
    }
    move(source, target, size, params) {
        return this.local_copy(source, target, size, params).then(()=> this.queue.run(() => this.S3.deleteObject({Bucket: (params.origin_bucket || this.bucket), Key: source}, ).promise()))
    }
    local_copy(source, target, size, params) {
        return this.restore(source, params).then(() => this.queue.run(() => {
            if (size > maxFileLength) return this.S3.copyObject({Bucket: params.bucket || this.bucket, Key: target, CopySource: encodeURI((params.origin_bucket || this.bucket) + "/" + source)}).promise()

            let map = [], id;
            return this.S3.createMultipartUpload({Bucket: this.bucket, Key: target, ContentType:  mime.lookup(source)}).promise()
                .then(response => {
                    id = response.UploadId;
                    let part = 0, percentage = 0, loaded = 0;
                    let partSize = params.partSize || 50 * 1024 * 1024;
                    while (size / partSize >= 10000) partSize *= 2;
                    let concurrency = params.concurrency || 8;
                    let requests_queue = require("parallel_limit")(concurrency);
                    let requests = [];
                    for (let start = 0, end = 0; end === size; start += partSize) {
                        end += partSize;
                        if (size - end < minPartSize) end = size;
                        part++;
                        let upload = (trials = 0) => this.S3.uploadPartCopy({Bucket: params.bucket || this.bucket, Key: target, PartNumber: part, UploadId: id, CopySource: encodeURI((params.origin_bucket || this.bucket) + "/" + source), CopySourceRange: `bytes=${start}-${end - 1}`}).promise()
                            .then(data => {
                                map.push({ETag: data.CopyPartResult.ETag, PartNumber: part});
                                if (params.publish) {
                                    loaded += end - start;
                                    let tmp = Math.round(loaded * 100 / size);
                                    if (percentage != tmp) {
                                        percentage = tmp;
                                        params.publish({current: loaded, total: size, percentage: percentage});
                                    }
                                }
                            })
                            .catch(err => {
                                this.logger.error("Error copying part: ", err);
                                if (trials++ < maxTrials) return upload(trials);
                                throw err;
                            })

                        requests.push(requests_queue.run(upload));
                    }
                    return Promise.all(requests);
                })
                .then(() => this.S3.completeMultipartUpload({Bucket: params.bucket || this.bucket, Key: target, MultipartUpload: {Parts: map}, UploadId: id}).promise())
                .catch(err => {
                    this.logger.error("Multipart Copy error: ", err);
                    let abort = (trials = 0) => this.S3.abortMultipartUpload({Bucket: params.bucket || this.bucket, Key: target, UploadId: id}).promise()
                        .then(() => this.S3.listParts({Bucket: params.bucket || this.bucket, Key: target, UploadId: id}).promise())
                        .then((parts) => {if (parts.Parts.length > 0) throw {pending_parts: true};})
                        .catch(err => {
                            if (trials++ < maxTrials) {
                                this.logger.error("Abort Multipart Copy error. Retrying... ", err);
                                return new Promise(resolve => setTimeout(resolve, 5000)).then(() => abort(trials));
                            }
                            this.logger.error("Abort Multipart Copy error. No more retries. There may be pending parts in the bucket.", err);
                        })
                    return abort().then(() => {throw err});
                })
        }));
    }
    tag(filename, params) {
        return this.queue.run(() => this.S3.putObjectTagging({Bucket: params.bucket || this.bucket, Key: filename, Tagging: {TagSet: params.tags}}).promise());
    }
    restore(source, params) {
        return this.stat(source, params)
            .then(data => {
                if (data.needs_restore) {
                    let key = path.posix.join(params.origin_bucket || params.bucket || this.bucket, source);
                    if (data.restoring) {
                        this.logger.info("Waiting for restore " + source);
                        if (!restores.hasOwnProperty(key)) restores[key] = new Promise(resolve => setTimeout(resolve, 10000))
                            .then(() => this.wait_restore_completed(source, params))
                            .then(stats => {
                                delete restores[key];
                                return stats;
                            })
                        if (params.wait_for_restore) return restores[key];
                        else throw "Restore not completed " + source;
                    }
                    if (params.restore === false) throw "Object is archived and restore was not requested";
                    if (data.storage_class === 'DEEP_ARCHIVE') params.tier = "Standard";
                    return this.restore_object(source, params)
                        .then(() => {
                            if (!restores.hasOwnProperty(key)) restores[key] = new Promise(resolve => setTimeout(resolve, 10000))
                                .then(() => this.wait_restore_completed(source, params))
                                .then(stats => {
                                    delete restores[key];
                                    return stats;
                                })
                            if (params.wait_for_restore) return restores[key];
                            else throw "Object was archived. Requested restore for " + source;
                        });
                }

                return data;
            })
            .catch(err => {
                this.logger.info("Error restoring object '" + source + "' on bucket '" + (params.origin_bucket || params.bucket || this.bucket) + "'", err)
                throw err;
            });
    }
    wait_restore_completed(source, params) {
        this.logger.info("Requesting restore of " + source);
        return this.stat(source, params)
            .then(data => {
                if (data.needs_restore && data.restoring) return new Promise(resolve => setTimeout(resolve, 10000)).then(() => this.wait_restore_completed(source, params));
                this.on_file_restore(params.origin_bucket || params.bucket || this.bucket, source);
                return data;
            });
    }
    restore_object(source, params) {
        return this.queue.run(() => this.S3.restoreObject({Bucket: params.origin_bucket || params.bucket || this.bucket, Key: source, RestoreRequest: {Days: params.days || 1, GlacierJobParameters: {Tier: params.tier}}}).promise())
            .catch(err => {
                this.logger.info("Error restoring object '" + source + "' on bucket '" + (params.origin_bucket || params.bucket || this.bucket) + "'", err)
                if (err?.code === 'RestoreAlreadyInProgress') return;
                if (err?.code === 'GlacierExpeditedRetrievalNotAvailable') {
                    params.tier = "Standard";
                    return this.restore_object(source, params);
                }
                throw err;
            });
    }
    walk(dirname, ignored, token) {
        return this.queue.run(() => this.S3.listObjectsV2({Bucket: this.bucket, Prefix: this.constructor.normalize_path(dirname), ContinuationToken: token}).promise())
            .then(data => {
                let list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    let {Key, Size} = list[i];
                    if (Key.match(ignored)) continue;
                    if (!Key.endsWith('/')) {
                        if (!this.fileObjects[Key] || (this.fileObjects[Key] && Size !== this.fileObjects[Key].size)) {
                            this.on_file_added(Key, {size: Size, mtime: list[i].LastModified, isDirectory: () => Key.endsWith('/')});
                            this.logger.info("AWS S3 walk adding: ", Key);
                        }
                        this.fileObjects[Key] = {last_seen: this.now, size: Size};
                    }
                }

                if (data.IsTruncated) return this.walk(dirname, ignored, data.NextContinuationToken);
            })
            .catch(err => {
                this.logger.error("AWS S3 walk failed with dirname: ", dirname, err);
                this.on_error(err);
            });
    }
    static normalize_path(dirname, is_filename) {
        return (dirname || "").replace(/[\\\/]+/g, "/").replace(/\/+$/, "").concat(is_filename ? "" : "/").replace(/^\/+/, "")
    }
    static filename(dirname, uri) {
        return uri.slice(dirname.length);
    }
    static path(dirname, filename) {
        return this.normalize_path(dirname) + filename;
    }
}
