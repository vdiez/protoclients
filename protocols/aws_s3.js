const path = require('path').posix;
const S3 = require('aws-sdk/clients/s3');
const mime = require('mime-types');
const qs = require('querystring');
const Stream = require('stream');
const parallel_limit = require('parallel_limit');
const Base = require('../base');

const {awsConfig, rootDirnames} = require('../config');

const maxTrials = 3;
const minPartSize = 5242880;

class AWS extends Base {
    static parameters ={
        access_key: {
            secret: true
        },
        bucket: {
            text: true
        },
        parallel: {
            number: true
        },
        polling: {
            boolean: true
        },
        polling_interval: {
            number: true
        },
        region: {
            text: true
        },
        secret: {
            secret: true
        }
    }

    constructor(params, logger) {
        super('aws_s3', params, logger);
        this.on_file_restore = () => {};
        this.restores = {};
    }

    static generate_id(params) {
        return JSON.stringify({
            protocol: 'aws_s3',
            accessKeyId: params.access_key,
            secretAccessKey: params.secret,
            region: params.region
        });
    }

    update_settings(params) {
        this.bucket = params.bucket;
        if (this.id() !== AWS.generate_id(params)) {
            this.S3 = new S3({
                accessKeyId: params.access_key,
                secretAccessKey: params.secret,
                region: params.region
            });
        }
        super.update_settings(params);
    }

    read(filename, params = {}) {
        filename = AWS.normalize_path(filename, true);
        return this.restore(filename, params).then(() => {
            return this.queue.run(slot => {
                let range;
                if (params.start || params.end) {
                    range = `bytes=${params.start || 0}-${params.end || ''}`;
                }
                this.logger.debug(`AWS S3 (slot ${slot}) read: `, filename);
                return this.S3.getObject({
                    Bucket: params.bucket || this.bucket,
                    Key: filename,
                    Range: range
                }).promise().then(data => Base.get_data(data.Body, params.encoding));
            });
        });
    }

    stat(filename, params = {}) {
        filename = AWS.normalize_path(filename, true);
        if (rootDirnames.includes(filename)) {
            return Promise.resolve({
                name: '',
                size: 0,
                mtime: new Date(),
                isDirectory: () => true
            });
        }
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) stat: `, filename);
            return this.S3.headObject({
                Bucket: params.bucket || this.bucket,
                Key: filename
            }).promise();
        }).then(data => {
            const restore = data.Restore?.match(/ongoing-request="(.+?)"(?:, expiry-date="(.+?)")?/);
            return {
                size: data.ContentLength,
                mtime: data.LastModified,
                storage_class: data.StorageClass,
                needs_restore: (data.StorageClass === 'GLACIER' || data.StorageClass === 'DEEP_ARCHIVE') && (!restore || restore?.[1] === 'true'),
                restoring: restore?.[1] === 'true',
                isDirectory: () => false
            };
        }).catch(err => {
            if (err.code === 'NotFound' && !filename.endsWith('/')) { // check if stating a dir
                return this.queue.run(slot => {
                    this.logger.debug(`AWS S3 (slot ${slot}) stat: `, filename);
                    return this.S3.listObjectsV2({
                        Bucket: params.bucket || this.bucket,
                        Prefix: filename
                    }).promise();
                }).then(data => {
                    return data.Contents?.length ? {
                        name: filename,
                        size: 0,
                        mtime: new Date(),
                        isDirectory: () => true
                    } : null;
                });
            }
            throw err;
        });
    }

    createReadStream(source, params = {}) {
        source = AWS.normalize_path(source, true);
        return this.restore(source, params).then(() => {
            let range;
            if (params.start || params.end) {
                range = `bytes=${params.start || 0}-${params.end || ''}`;
            }
            const control_release = true;
            return this.queue.run((slot, slot_control) => {
                this.logger.debug(`AWS S3 (slot ${slot}) create stream from: `, source);
                slot_control.keep_busy = true;
                const stream = this.S3.getObject({
                    Bucket: params.bucket || this.bucket,
                    Key: source,
                    Range: range
                }).createReadStream();
                stream.on('error', slot_control.release_slot);
                stream.on('end', slot_control.release_slot);
                stream.on('close', slot_control.release_slot);
                return stream;
            }, control_release);
        });
    }

    createWriteStream(target, params = {}) {
        target = AWS.normalize_path(target, true);
        const control_release = true;
        return this.queue.run((slot, slot_control) => {
            return new Promise((resolve, reject) => {
                this.logger.debug(`AWS S3 (slot ${slot}) create write stream to: `, target);
                slot_control.keep_busy = true;
                const stream = {
                    passThrough: new Stream.PassThrough()
                };
                this.S3.upload({
                    Bucket: params.bucket || this.bucket,
                    Key: target,
                    Body: stream,
                    ContentType: mime.lookup(target),
                    StorageClass: params.storage_class,
                    Tagging: qs.stringify(params.tags)
                }, (err) => {
                    if (err) {
                        reject(err);
                    }
                    slot_control.release_slot();
                });
                resolve(stream);
            });
        }, control_release);
    }

    mkdir(dir, params = {}) {
        return this.queue.run(slot => {
            const Bucket = params.bucket || this.bucket;
            this.logger.debug(`AWS S3 (slot ${slot}) mkdir/create bucket: `, Bucket);
            return this.S3.createBucket({Bucket}).promise().then(() => {
                return this.S3.putObject({
                    Bucket,
                    Key: `${dir}/`
                }).promise();
            }).catch(err => this.logger.error('Could not create bucket: ', err));
        });
    }

    write(target, contents = '', params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) upload to: `, target);
            return this.S3.putObject({
                Bucket: params.bucket || this.bucket,
                Key: target,
                Body: contents
            }).promise();
        });
    }

    copy(source, target, streams, size, params) {
        if (!streams.readStream) {
            return this.local_copy(source, target, size, params);
        }
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) upload stream to: `, target);
            return new Promise((resolve, reject) => {
                let partSize = params.partSize || awsConfig.basePartSize;
                while (size / partSize > awsConfig.maxParts) {
                    partSize *= 2;
                }
                const options = params.options || {
                    partSize,
                    queueSize: params.concurrency || 8
                };
                const result = this.S3.upload({
                    Bucket: params.bucket || this.bucket,
                    Key: target,
                    Body: streams.passThrough,
                    ContentType: mime.lookup(source),
                    StorageClass: params.storage_class,
                    Tagging: qs.stringify(params.tags)
                }, options, (err) => (err ? reject(err) : resolve()));
                streams.readStream.pipe(streams.passThrough);
                streams.readStream.on('error', err => reject(err));
                streams.passThrough.on('error', err => reject(err));
                if (params.publish) {
                    let percentage = 0;
                    result.on('httpUploadProgress', event => {
                        const tmp = Math.round((event.loaded * 100) / (event.total || size));
                        if (percentage !== tmp) {
                            percentage = tmp;
                            params.publish({
                                current: event.loaded,
                                total: event.total || size,
                                percentage
                            });
                        }
                    });
                }
            }).then(() => {
                return params.make_public
                    ? this.S3.putObjectAcl({
                        Bucket: params.bucket || this.bucket,
                        ACL: 'public-read',
                        Key: target
                    }).promise()
                    : null;
            });
        });
    }

    remove(target, params) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) remove: `, target);
            this.S3.deleteObject({
                Bucket: params?.bucket || this.bucket,
                Key: target
            }).promise();
        });
    }

    move(source, target, size, params) {
        return this.local_copy(source, target, size, params).then(() => this.remove(source, params));
    }

    local_copy(source, target, size, params) {
        const Bucket = params.bucket || this.bucket;
        return this.restore(source, params).then(() => this.queue.run(slot => {
            if (size < awsConfig.maxFileLength) {
                this.logger.debug(`AWS S3 (slot ${slot}) atomic local copy: `, source, target);
                return this.S3.copyObject({
                    Bucket,
                    Key: target,
                    CopySource: encodeURI(`${params.source_bucket || Bucket}/${source}`)
                }).promise();
            }

            const map = [];
            let id;
            this.logger.debug(`AWS S3 (slot ${slot}) multipart local copy: `, source, target);
            return this.S3.createMultipartUpload({
                Bucket: this.bucket,
                Key: target,
                ContentType: mime.lookup(source)
            }).promise().then(response => {
                id = response.UploadId;
                let partSize = params.partSize || awsConfig.basePartSize;
                while (size / partSize >= awsConfig.maxParts) {
                    partSize *= 2;
                }
                const concurrency = params.concurrency || awsConfig.concurrency;
                const requests_queue = parallel_limit(concurrency);
                const requests = [];
                let percentage = 0;
                let loaded = 0;
                for (let start = 0, end = 0, part = 1; end < size; start += partSize, part += 1) {
                    end += partSize;
                    if (size - end < minPartSize) {
                        end = size;
                    }
                    const upload = (trials = 0) => this.S3.uploadPartCopy({
                        Bucket,
                        Key: target,
                        PartNumber: part,
                        UploadId: id,
                        CopySource: encodeURI(`${params.source_bucket || Bucket}/${source}`),
                        CopySourceRange: `bytes=${start}-${end - 1}`
                    }).promise().then(data => {
                        map[part - 1] = {
                            ETag: data.CopyPartResult.ETag,
                            PartNumber: part
                        };
                        if (params.publish) {
                            loaded += end - start;
                            const tmp = Math.round((loaded * 100) / size);
                            if (percentage !== tmp) {
                                percentage = tmp;
                                params.publish({
                                    current: loaded,
                                    total: size,
                                    percentage
                                });
                            }
                        }
                    }).catch(err => {
                        this.logger.error('Error copying part: ', err);
                        if (trials < maxTrials) {
                            trials += 1;
                            return upload(trials);
                        }
                        throw err;
                    });
                    requests.push(requests_queue.run(upload));
                }
                return Promise.all(requests);
            }).then(() => {
                return this.S3.completeMultipartUpload({
                    Bucket,
                    Key: target,
                    MultipartUpload: {
                        Parts: map
                    },
                    UploadId: id
                }).promise();
            }).catch(err => {
                this.logger.error('Multipart Copy error: ', err);
                const S3Parts = {
                    Bucket,
                    Key: target,
                    UploadId: id
                };
                const abort = (trials = 0) => {
                    return this.S3.abortMultipartUpload(S3Parts).promise().then(() => {
                        return this.S3.listParts(S3Parts).promise();
                    }).then((parts) => {
                        if (parts.Parts.length > 0) {
                            throw {pending_parts: true};
                        }
                    }).catch(abort_err => {
                        if (abort_err?.code === 'NoSuchUpload') {
                            return;
                        }
                        if (trials < maxTrials) {
                            trials += 1;
                            this.logger.error('Abort Multipart Copy error. Retrying... ', abort_err);
                            new Promise(resolve => setTimeout(resolve, 5000)).then(() => abort(trials));
                            return;
                        }
                        this.logger.error('Abort Multipart Copy error. No more retries. There may be pending parts in the bucket.', abort_err);
                    });
                };
                return abort().then(() => {throw err;});
            });
        }));
    }

    tag(filename, tags, params) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) tag: `, filename, tags);
            return this.S3.putObjectTagging({
                Bucket: params.bucket || this.bucket,
                Key: filename,
                Tagging: {
                    TagSet: tags
                }
            }).promise();
        });
    }

    restore(source, params) {
        const on_file_restore = params.on_file_restore || this.on_file_restore;
        const bucket = params.bucket || this.bucket;
        return this.stat(source, params).then(data => {
            if (!data?.needs_restore) {
                return data;
            }
            const key = path.join(bucket, source);
            return Promise.resolve().then(() => {
                if (!data.restoring) {
                    if (params.restore === false) {
                        throw 'Object is archived and restore was not requested';
                    }
                    if (data.storage_class === 'DEEP_ARCHIVE') {
                        params.tier = 'Standard';
                    }
                    return this.restore_object(source, params);
                }
            }).then(() => {
                if (!this.restores?.[key]) {
                    this.restores[key] = new Promise(resolve => setTimeout(resolve, 10000))
                        .then(() => this.wait_restore_completed(source, params))
                        .then(stats => {
                            delete this.restores[key];
                            return stats;
                        });
                }
                if (!params.wait_for_restore) {
                    throw data.restoring ? `Restore not completed ${source}` : `Object was archived. Requested restore for ${source}`;
                }
                return on_file_restore('start', bucket, source);
            }).then(() => {
                return this.restores[key];
            }).then(() => {
                return on_file_restore('finish', bucket, source);
            }).then(() => data);
        }).catch(err => {
            this.logger.error(`Error restoring object '${source}' on bucket '${bucket}'`, err);
            throw err;
        });
    }

    wait_restore_completed(source, params) {
        return this.stat(source, params).then(data => {
            return (data.needs_restore && data.restoring)
                ? new Promise(resolve => setTimeout(resolve, 10000)).then(() => this.wait_restore_completed(source, params))
                : data;
        });
    }

    restore_object(source, params) {
        const Bucket = params.bucket || this.bucket;
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) restore: `, source);
            return this.S3.restoreObject({
                Bucket,
                Key: source,
                RestoreRequest: {
                    Days: params.days || 1,
                    GlacierJobParameters: {
                        Tier: params.tier || 'Expedited'
                    }
                }
            }).promise();
        }).catch(err => {
            this.logger.error(`Error restoring object '${source}' on bucket '${Bucket}'`, err);
            if (err?.code === 'RestoreAlreadyInProgress') return;
            if (err?.code === 'GlacierExpeditedRetrievalNotAvailable') {
                params.tier = 'Standard';
                return this.restore_object(source, params);
            }
            throw err;
        });
    }

    list(dirname, params, token, results = [], directories = []) {
        dirname = AWS.normalize_path(dirname);
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) list: `, dirname);
            return this.S3.listObjectsV2({
                Bucket: params.bucket || this.bucket,
                Prefix: dirname,
                ContinuationToken: token
            }).promise();
        }).then(data => {
            const list = data.Contents || [];
            for (let i = 0; i < list.length; i += 1) {
                const {Size, LastModified} = list[i];
                let Key = AWS.get_filename(dirname, list[i].Key);
                const slash_pos = Key.indexOf('/');
                if (slash_pos < 0) {
                    results.push({
                        name: Key,
                        size: Size,
                        mtime: LastModified,
                        isDirectory: () => false
                    });
                } else {
                    Key = Key.slice(0, slash_pos);
                    if (!directories.includes(Key)) {
                        directories.push(Key);
                        results.push({
                            name: Key,
                            size: Size,
                            mtime: LastModified,
                            isDirectory: () => true
                        });
                    }
                }
            }
            return data.IsTruncated ? this.list(dirname, params, data.NextContinuationToken, results, directories) : results;
        });
    }

    walk({dirname, bucket, ignored, on_file, on_error, token}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) list: `, dirname);
            return this.S3.listObjectsV2({
                Bucket: bucket || this.bucket,
                Prefix: AWS.normalize_path(dirname),
                ContinuationToken: token
            }).promise();
        }).then(data => {
            const list = data.Contents || [];
            for (let i = 0; i < list.length; i += 1) {
                const {Key, Size, LastModified} = list[i];
                if (!Key.match(ignored) && !Key.endsWith('/')) {
                    on_file(Key, {
                        size: Size,
                        mtime: LastModified,
                        isDirectory: () => false
                    });
                }
            }
            return data.IsTruncated ? this.walk({
                dirname,
                bucket,
                ignored,
                on_file,
                on_error,
                token: data.NextContinuationToken
            }) : null;
        }).catch(on_error);
    }

    static normalize_path(uri, is_filename) {
        uri = Base.normalize_path(uri);
        return rootDirnames.includes(uri) ? '' : uri.replace(/\/+$/, '').concat(is_filename ? '' : '/').replace(/^\/+/, '');
    }
}

module.exports = AWS;
