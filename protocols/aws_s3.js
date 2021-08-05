const maxFileLength = 5368709120;
const maxTrials = 3;
const minPartSize = 5242880;
let path = require('path');
let S3 = require('aws-sdk/clients/s3');
let mime = require('mime-types');
let base = require("../base");
let qs = require("querystring");

module.exports = class extends base {
    static parameters = {
        access_key: {type: "secret"},
        secret: {type: "secret"},
        bucket: {type: "text"},
        region: {type: "text"},
        ...base.parameters
    };
    constructor(params, logger) {
        super(params, logger, "aws_s3");
        this.on_file_restore = () => {};
        this.restores = {};
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'aws_s3', accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
    }
    update_settings(params) {
        this.bucket = params.bucket;
        if (this.id() !== this.constructor.generate_id(params)) this.S3 = new S3({accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
        super.update_settings(params);
    }
    read(filename, params = {}) {
        filename = this.constructor.normalize_path(filename, true);
        return this.restore(filename, params).then(() => this.queue.run(slot => {
            let range;
            if (params.start || params.end) range = `bytes=${params.start || 0}-${params.end || ""}`;
            this.logger.debug("AWS S3 (slot " + slot + ") read: ", filename);
            return this.S3.getObject({Bucket: params.bucket || this.bucket, Key: filename, Range: range}).promise().then(data => this.constructor.get_data(data.Body, params.encoding));
        }));
    }
    stat(filename, params = {}) {
        filename = this.constructor.normalize_path(filename, true);
        if (filename === "." || filename === "/" || filename === "./" || filename === "") return Promise.resolve({
            name: '',
            size: 0,
            mtime: new Date(),
            isDirectory: () => true
        })
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") stat: ", filename);
            return this.S3.headObject({Bucket: params.bucket || this.bucket, Key: filename}).promise()
        })
        .then(data => {
            let restore = data.Restore?.match(/ongoing-request="(.+?)"(?:, expiry-date="(.+?)")?/)
            return {
                size: data.ContentLength,
                mtime: data.LastModified,
                storage_class: data.StorageClass,
                needs_restore: (data.StorageClass === 'GLACIER' || data.StorageClass === 'DEEP_ARCHIVE') && (!restore || restore?.[1] === 'true'),
                restoring: restore?.[1] === 'true',
                isDirectory: () => false
            }
        })
        .catch(err => {
            if (err.code === "NotFound" && !filename.endsWith('/')) { //check if stating a dir
                return this.queue.run(slot => {
                    this.logger.debug("AWS S3 (slot " + slot + ") stat: ", filename);
                    return this.S3.listObjectsV2({Bucket: params.bucket || this.bucket, Prefix: filename}).promise();
                })
                .then(data => {
                    if (data.Contents?.length) return {
                        name: filename,
                        size: 0,
                        mtime: new Date(),
                        isDirectory: () => true
                    }
                });
            }
            throw err;
        });
    }
    createReadStream(source, params = {}) {
        source = this.constructor.normalize_path(source, true);
        return this.restore(source, params)
            .then(() => {
                let range;
                if (params.start || params.end) range = `bytes=${params.start || 0}-${params.end || ""}`;
                return this.queue.run((slot, slot_control) => {
                    this.logger.debug("AWS S3 (slot " + slot + ") create stream from: ", source);
                    let stream = this.S3.getObject({Bucket: params.bucket || this.bucket, Key: source, Range: range}).createReadStream()
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('end', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    return stream;
                }, true);
            });
    }
    createWriteStream(target, params = {}) {
        target = this.constructor.normalize_path(target, true);
        return this.queue.run((slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug("AWS S3 (slot " + slot + ") create write stream to: ", target);
            slot_control.keep_busy = true;
            let stream = {passThrough: new (require('stream')).PassThrough()};
            this.S3.upload({Bucket: params.bucket || this.bucket, Key: target, Body: stream, ContentType: mime.lookup(target), StorageClass: params.storage_class, Tagging: qs.stringify(params.tags)}, (err,data) => {
                if (err) reject(err);
                slot_control.release_slot();
            });
            resolve(stream);
        }), true)
    }
    mkdir(dir, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") mkdir/create bucket: ", params.bucket || this.bucket);
            return this.S3.createBucket({Bucket: params.bucket || this.bucket}).promise()
                .then(() => this.S3.putObject({Bucket: params.bucket || this.bucket, Key: dir + '/'}).promise())
                .catch(err => this.logger.error("Could not create bucket: ", err))
        })
    }
    write(target, contents = '', params = {}) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") upload to: ", target);
            return this.S3.putObject({Bucket: params.bucket || this.bucket, Key: target, Body: contents}).promise()
        })
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) return this.local_copy(source, target, size, params);
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") upload stream to: ", target);
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
                if (params.make_public) return this.S3.putObjectAcl({Bucket: params.bucket || this.bucket, ACL: "public-read", Key: target}).promise();
            })
        })
    }
    remove(target, params) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") remove: ", target);
            this.S3.deleteObject({Bucket: params?.bucket || this.bucket, Key: target}).promise()
        });
    }
    move(source, target, size, params) {
        return this.local_copy(source, target, size, params).then(()=> this.remove(source, params))
    }
    local_copy(source, target, size, params) {
        return this.restore(source, params).then(() => this.queue.run(slot => {
            if (size < maxFileLength) {
                this.logger.debug("AWS S3 (slot " + slot + ") atomic local copy: ", source, target);
                return this.S3.copyObject({Bucket: params.bucket || this.bucket, Key: target, CopySource: encodeURI((params.source_bucket || params.bucket || this.bucket) + "/" + source)}).promise()
            }

            let map = [], id;
            this.logger.debug("AWS S3 (slot " + slot + ") multipart local copy: ", source, target);
            return this.S3.createMultipartUpload({Bucket: this.bucket, Key: target, ContentType:  mime.lookup(source)}).promise()
                .then(response => {
                    id = response.UploadId;
                    let percentage = 0, loaded = 0;
                    let partSize = params.partSize || 50 * 1024 * 1024;
                    while (size / partSize >= 10000) partSize *= 2;
                    let concurrency = params.concurrency || 8;
                    let requests_queue = require("parallel_limit")(concurrency);
                    let requests = [];
                    for (let start = 0, end = 0, part = 1; end < size; start += partSize, part++) {
                        end += partSize;
                        if (size - end < minPartSize) end = size;
                        let upload = (trials = 0) => this.S3.uploadPartCopy({Bucket: params.bucket || this.bucket, Key: target, PartNumber: part, UploadId: id, CopySource: encodeURI((params.source_bucket || params.bucket || this.bucket) + "/" + source), CopySourceRange: `bytes=${start}-${end - 1}`}).promise()
                            .then(data => {
                                map[part - 1] = {ETag: data.CopyPartResult.ETag, PartNumber: part};
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
                        .catch(abort_err => {
                            if (abort_err?.code === "NoSuchUpload") return;
                            if (trials++ < maxTrials) {
                                this.logger.error("Abort Multipart Copy error. Retrying... ", abort_err);
                                return new Promise(resolve => setTimeout(resolve, 5000)).then(() => abort(trials));
                            }
                            this.logger.error("Abort Multipart Copy error. No more retries. There may be pending parts in the bucket.", abort_err);
                        })
                    return abort().then(() => {throw err});
                })
        }));
    }
    tag(filename, tags, params) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") tag: ", filename, tags);
            return this.S3.putObjectTagging({Bucket: params.bucket || this.bucket, Key: filename, Tagging: {TagSet: tags}}).promise()
        });
    }
    restore(source, params) {
        return this.stat(source, params)
            .then(data => {
                if (!data?.needs_restore) return data;
                let key = path.posix.join(params.bucket || this.bucket, source);
                return Promise.resolve()
                    .then(() => {
                        if (!data.restoring) {
                            if (params.restore === false) throw "Object is archived and restore was not requested";
                            if (data.storage_class === 'DEEP_ARCHIVE') params.tier = "Standard";
                            return this.restore_object(source, params)
                        }
                    })
                    .then(() => {
                        if (!this.restores.hasOwnProperty(key)) this.restores[key] = new Promise(resolve => setTimeout(resolve, 10000))
                            .then(() => this.wait_restore_completed(source, params))
                            .then(stats => {
                                delete this.restores[key];
                                return stats;
                            })
                        if (!params.wait_for_restore) throw data.restoring ? "Restore not completed " + source : "Object was archived. Requested restore for " + source
                        return (params.on_file_restore || this.on_file_restore)("start", params.bucket || this.bucket, source)
                    })
                    .then(() => this.restores[key])
                    .then(() => (params.on_file_restore || this.on_file_restore)("finish", params.bucket || this.bucket, source))
                    .then(() => data)
            })
            .catch(err => {
                this.logger.error("Error restoring object '" + source + "' on bucket '" + (params.bucket || this.bucket) + "'", err)
                throw err;
            });
    }
    wait_restore_completed(source, params) {
        return this.stat(source, params)
            .then(data => {
                if (data.needs_restore && data.restoring) return new Promise(resolve => setTimeout(resolve, 10000)).then(() => this.wait_restore_completed(source, params));
                return data;
            });
    }
    restore_object(source, params) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") restore: ", source);
            return this.S3.restoreObject({Bucket: params.bucket || this.bucket, Key: source, RestoreRequest: {Days: params.days || 1, GlacierJobParameters: {Tier: params.tier || "Expedited"}}}).promise()
        })
            .catch(err => {
                this.logger.error("Error restoring object '" + source + "' on bucket '" + (params.bucket || this.bucket) + "'", err)
                if (err?.code === 'RestoreAlreadyInProgress') return;
                if (err?.code === 'GlacierExpeditedRetrievalNotAvailable') {
                    params.tier = "Standard";
                    return this.restore_object(source, params);
                }
                throw err;
            });
    }
    list(dirname, {bucket}, token, results = [], directories = []) {
        dirname = this.constructor.normalize_path(dirname);
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") list: ", dirname);
            return this.S3.listObjectsV2({Bucket: bucket || this.bucket, Prefix: dirname, ContinuationToken: token}).promise();
        })
            .then(data => {
                let list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    let {Key, Size, LastModified} = list[i];
                    Key = this.constructor.get_filename(dirname, Key);
                    let slash_pos = Key.indexOf('/');
                    if (slash_pos < 0) results.push({name: Key, size: Size, mtime: LastModified, isDirectory: () => false});
                    else {
                        Key = Key.slice(0, slash_pos)
                        if (!directories.includes(Key)) {
                            directories.push(Key)
                            results.push({name: Key, size: Size, mtime: LastModified, isDirectory: () => true});
                        }
                    }
                }
                if (data.IsTruncated) return this.list(dirname, {bucket}, data.NextContinuationToken, results, directories);
                return results;
            });
    }
    walk({dirname, bucket, ignored, on_file, on_error, token}) {
        return this.queue.run(slot => {
            this.logger.debug("AWS S3 (slot " + slot + ") list: ", dirname);
            return this.S3.listObjectsV2({Bucket: bucket || this.bucket, Prefix: this.constructor.normalize_path(dirname), ContinuationToken: token}).promise();
        })
            .then(data => {
                let list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    let {Key, Size, LastModified} = list[i];
                    if (Key.match(ignored)) continue;
                    if (!Key.endsWith('/')) on_file(Key, {size: Size, mtime: LastModified, isDirectory: () => false});
                }

                if (data.IsTruncated) return this.walk({dirname, bucket, ignored, on_file, on_error, token: data.NextContinuationToken});
            })
            .catch(on_error);
    }
    static normalize_path(uri, is_filename) {
        uri = super.normalize_path(uri);
        if (uri === "." || uri === "/" || uri === "./" || uri === "") return '';
        return uri.replace(/\/+$/, "").concat(is_filename ? "" : "/").replace(/^\/+/, "")
    }
}
