const maxFileLength = 5368709120;
const maxTrials = 3;
const minPartSize = 5242880;
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const mime = require('mime-types');
const qs = require('querystring');
const Stream = require('stream');
const base = require('../base');

module.exports = class extends base {
    static parameters = {
        access_key: {type: 'secret'},
        secret: {type: 'secret'},
        bucket: {type: 'text'},
        region: {type: 'text'},
        ...base.parameters
    };

    constructor(params, logger) {
        super(params, logger, 'aws_s3');
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
            if (params.start || params.end) range = `bytes=${params.start || 0}-${params.end || ''}`;
            this.logger.debug(`AWS S3 (slot ${slot}) read ${filename}`);
            return this.S3.getObject({Bucket: params.bucket || this.bucket, Key: filename, Range: range}).promise().then(data => this.constructor.get_data(data.Body, params.encoding));
        }));
    }

    stat(filename, params = {}) {
        filename = this.constructor.normalize_path(filename, true);
        if (filename === '.' || filename === '/' || filename === './' || filename === '') return Promise.resolve({
            name: '',
            size: 0,
            mtime: new Date(),
            isDirectory: () => true
        });
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) stat ${filename}`);
            return this.S3.headObject({Bucket: params.bucket || this.bucket, Key: filename}).promise();
        })
            .then(data => {
                const restore = data.Restore?.match(/ongoing-request="(.+?)"(?:, expiry-date="(.+?)")?/);
                return {
                    size: data.ContentLength,
                    mtime: data.LastModified,
                    storage_class: data.StorageClass,
                    needs_restore: (data.StorageClass === 'GLACIER' || data.StorageClass === 'DEEP_ARCHIVE') && (!restore || restore?.[1] === 'true'),
                    restoring: restore?.[1] === 'true',
                    isDirectory: () => false
                };
            })
            .catch(err => {
                if (err.code === 'NotFound' && !filename.endsWith('/')) { //check if stating a dir
                    return this.queue.run(slot => {
                        this.logger.debug(`AWS S3 (slot ${slot}) stat: `, filename);
                        return this.S3.listObjectsV2({Bucket: params.bucket || this.bucket, Prefix: filename}).promise();
                    })
                        .then(data => {
                            if (data.Contents?.length) return {
                                name: filename,
                                size: 0,
                                mtime: new Date(),
                                isDirectory: () => true
                            };
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
                if (params.start || params.end) range = `bytes=${params.start || 0}-${params.end || ''}`;
                return this.queue.run((slot, slot_control) => {
                    this.logger.debug(`AWS S3 (slot ${slot}) create stream from ${source}`);
                    const stream = this.S3.getObject({Bucket: params.bucket || this.bucket, Key: source, Range: range}).createReadStream();
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
            this.logger.debug(`AWS S3 (slot ${slot}) create write stream to ${target}`);
            slot_control.keep_busy = true;
            let partSize = params.partSize || 50 * 1024 * 1024;
            const size = params?.size || 0;
            while (size / partSize > 10000) partSize *= 2;
            const options = {partSize, queueSize: params.concurrency || 8};
            const stream = new Stream.PassThrough();
            const result = this.S3.upload({Bucket: params.bucket || this.bucket, Key: target, Body: stream, ContentType: mime.lookup(target), StorageClass: params.storage_class, Tagging: qs.stringify(params.tags)}, options, err => {
                if (err) reject(err);
                slot_control.release_slot();
            });
            if (typeof params.publish === 'function') {
                result.on('httpUploadProgress', event => {params.publish(event.loaded);});
                params.publish.took_publish_control = true;
            }
            resolve(stream);
        }), true);
    }

    mkdir(dir, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) mkdir/create bucket ${params.bucket || this.bucket}`);
            return this.S3.createBucket({Bucket: params.bucket || this.bucket}).promise()
                .then(() => this.S3.putObject({Bucket: params.bucket || this.bucket, Key: `${dir}/`}).promise())
                .catch(err => this.logger.error('Could not create bucket: ', err));
        });
    }

    write(target, contents = '', params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) upload to ${target}`);
            return this.S3.putObject({Bucket: params.bucket || this.bucket, Key: target, Body: contents}).promise();
        });
    }

    set_acl(target, acl, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) set ACL to ${target}`);
            return this.S3.putObjectAcl({Bucket: params.bucket || this.bucket, ACL: acl, Key: target}).promise();
        });
    }

    remove(target, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) remove: ${target}`);
            return this.S3.deleteObject({Bucket: params.bucket || this.bucket, Key: target}).promise();
        });
    }

    move(source, target, params) {
        return this.copy(source, target, params).then(() => this.remove(source, params));
    }

    copy_part(source, target, params = {}) {
        return this.queue.run(slot => {
            const {bucket, source_bucket, id, start, end, part, trials = 0} = params;
            this.logger.debug(`AWS S3 (slot ${slot}) copy multipart from ${source} to ${target}. ID:${id}, part ${part} (${start}-${end})`);
            return this.S3.uploadPartCopy({Bucket: bucket || this.bucket, Key: target, PartNumber: part, UploadId: id, CopySource: encodeURI(`${source_bucket || bucket || this.bucket}/${source}`), CopySourceRange: `bytes=${start}-${end - 1}`}).promise()
                .then(data => ({etag: data.CopyPartResult.ETag, part, increase: end - start}))
                .catch(err => {
                    this.logger.error('Error copying part: ', err);
                    if (trials + 1 < maxTrials) return this.copy_part(source, target, {...params, trials: trials + 1});
                    throw err;
                });
        });
    }

    abort_multipart(target, params = {}) {
        return this.queue.run(slot => {
            const {bucket, id, trials = 0} = params;
            this.logger.debug(`AWS S3 (slot ${slot}) aborting multipart to ${target}. ID:${id}`);
            this.S3.abortMultipartUpload({Bucket: bucket || this.bucket, Key: target, UploadId: id}).promise()
                .then(() => this.S3.listParts({Bucket: bucket || this.bucket, Key: target, UploadId: id}).promise())
                .then((parts) => {
                    if (parts.Parts.length > 0) throw {pending_parts: true};
                })
                .catch(abort_err => {
                    if (abort_err?.code === 'NoSuchUpload') return;
                    if (trials + 1 < maxTrials) {
                        this.logger.error('Abort Multipart Copy error. Retrying... ', abort_err);
                        return new Promise(resolve => {
                            setTimeout(resolve, 5000);
                        }).then(() => this.abort_multipart(target, {...params, trials: trials + 1}));
                    }
                    this.logger.error('Abort Multipart Copy error. No more retries. There may be pending parts in the bucket.', abort_err);
                });
        });
    }

    copy(source, target, params = {}) {
        return this.restore(source, params).then(() => {
            const size = params?.size;
            if (size < maxFileLength) {
                return this.queue.run(slot => {
                    this.logger.debug(`AWS S3 (slot ${slot}) atomic local copy from ${source} to ${target}`);
                    return this.S3.copyObject({Bucket: params.bucket || this.bucket, Key: target, CopySource: encodeURI(`${params.source_bucket || params.bucket || this.bucket}/${source}`)}).promise();
                });
            }

            const map = [];
            let id;

            return this.queue.run(slot => {
                this.logger.debug(`AWS S3 (slot ${slot}) multipart local copy from ${source} to ${target}`);
                return this.S3.createMultipartUpload({Bucket: this.bucket, Key: target, ContentType: mime.lookup(source)}).promise();
            })
                .then(response => {
                    let loaded = 0;
                    const publish = (typeof params.publish === 'function') ? (increase) => {
                        loaded += increase;
                        params.publish(loaded);
                    } : () => {};
                    id = response.UploadId;
                    let partSize = params.partSize || 50 * 1024 * 1024;
                    while (size / partSize >= 10000) partSize *= 2;
                    const requests = [];
                    for (let start = 0, end = 0, part = 1; end < size; start += partSize, part++) {
                        end += partSize;
                        if (size - end < minPartSize) end = size;

                        requests.push(this.copy_part(source, target, {...params, id, start, end, part})
                            .then(data => {
                                map[data.part - 1] = {ETag: data.etag, PartNumber: data.part};
                                publish(data.increase);
                            }));
                    }
                    return Promise.all(requests);
                })
                .then(() => this.queue.run(slot => {
                    this.logger.debug(`AWS S3 (slot ${slot}) completing multipart copy from ${source} to ${target}`);
                    return this.S3.completeMultipartUpload({Bucket: params.bucket || this.bucket, Key: target, MultipartUpload: {Parts: map}, UploadId: id}).promise();
                }))
                .catch(err => {
                    this.logger.error('Multipart Copy error: ', err);
                    return this.abort_multipart(target, {...params, id}).then(() => {throw err;});
                });
        });
    }

    tag(filename, tags, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) tag ${filename}: `, tags);
            return this.S3.putObjectTagging({Bucket: params.bucket || this.bucket, Key: filename, Tagging: {TagSet: tags}}).promise();
        });
    }

    restore(source, params = {}) {
        return this.stat(source, params)
            .then(data => {
                if (!data?.needs_restore) return data;
                const key = path.posix.join(params.bucket || this.bucket, source);
                return Promise.resolve()
                    .then(() => {
                        if (!data.restoring) {
                            if (params.restore === false) throw 'Object is archived and restore was not requested';
                            if (data.storage_class === 'DEEP_ARCHIVE') params.tier = 'Standard';
                            return this.restore_object(source, params);
                        }
                    })
                    .then(() => {
                        if (!this.restores.hasOwnProperty(key)) this.restores[key] = new Promise(resolve => {setTimeout(resolve, 10000);})
                            .then(() => this.wait_restore_completed(source, params))
                            .then(stats => {
                                delete this.restores[key];
                                return stats;
                            });
                        if (!params.wait_for_restore) throw data.restoring ? `Restore not completed ${source}` : `Object was archived. Requested restore for ${source}`;
                        return (params.on_file_restore || this.on_file_restore)('start', params.bucket || this.bucket, source);
                    })
                    .then(() => this.restores[key])
                    .then(() => (params.on_file_restore || this.on_file_restore)('finish', params.bucket || this.bucket, source))
                    .then(() => data);
            })
            .catch(err => {
                this.logger.error(`Error restoring object '${source}' on bucket '${params.bucket || this.bucket}'`, err);
                throw err;
            });
    }

    wait_restore_completed(source, params) {
        return this.stat(source, params)
            .then(data => {
                if (data.needs_restore && data.restoring) return new Promise(resolve => {setTimeout(resolve, 10000);}).then(() => this.wait_restore_completed(source, params));
                return data;
            });
    }

    restore_object(source, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) restore: ${source}`);
            return this.S3.restoreObject({Bucket: params.bucket || this.bucket, Key: source, RestoreRequest: {Days: params.days || 1, GlacierJobParameters: {Tier: params.tier || 'Expedited'}}}).promise();
        })
            .catch(err => {
                this.logger.error(`Error restoring object '${source}' on bucket '${params.bucket || this.bucket}'`, err);
                if (err?.code === 'RestoreAlreadyInProgress') return;
                if (err?.code === 'GlacierExpeditedRetrievalNotAvailable') {
                    params.tier = 'Standard';
                    return this.restore_object(source, params);
                }
                throw err;
            });
    }

    list(dirname, {bucket}, token, results = [], directories = []) {
        dirname = this.constructor.normalize_path(dirname);
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) list ${dirname}`);
            return this.S3.listObjectsV2({Bucket: bucket || this.bucket, Prefix: dirname, ContinuationToken: token}).promise();
        })
            .then(data => {
                const list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    let Key = this.constructor.get_filename(dirname, list[i].Key);
                    const slash_pos = Key.indexOf('/');
                    if (slash_pos < 0) results.push({name: Key, size: list[i].Size, mtime: list[i].LastModified, isDirectory: () => false});
                    else {
                        Key = Key.slice(0, slash_pos);
                        if (!directories.includes(Key)) {
                            directories.push(Key);
                            results.push({name: Key, size: list[i].Size, mtime: list[i].LastModified, isDirectory: () => true});
                        }
                    }
                }
                if (data.IsTruncated) return this.list(dirname, {bucket}, data.NextContinuationToken, results, directories);
                return results;
            });
    }

    walk({dirname, bucket, ignored, on_file, on_error, token}) {
        return this.queue.run(slot => {
            this.logger.debug(`AWS S3 (slot ${slot}) list ${dirname}`);
            return this.S3.listObjectsV2({Bucket: bucket || this.bucket, Prefix: this.constructor.normalize_path(dirname), ContinuationToken: token}).promise();
        })
            .then(data => {
                const list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    const {Key, Size, LastModified} = list[i];
                    if (Key.match(ignored)) continue;
                    if (!Key.endsWith('/')) on_file(Key, {size: Size, mtime: LastModified, isDirectory: () => false});
                }

                if (data.IsTruncated) return this.walk({dirname, bucket, ignored, on_file, on_error, token: data.NextContinuationToken});
            })
            .catch(on_error);
    }

    static normalize_path(uri, is_filename) {
        uri = super.normalize_path(uri);
        if (uri === '.' || uri === '/' || uri === './' || uri === '') return '';
        return uri.replace(/\/+$/, '').concat(is_filename ? '' : '/').replace(/^\/+/, '');
    }
};
