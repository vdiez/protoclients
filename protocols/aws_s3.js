let S3 = require('aws-sdk/clients/s3');
let mime = require('mime-types');
let base = require("../base");

module.exports = class extends base {
    static parameters = ["polling", "polling_interval", "access_key", "secret", "region", "bucket"];
    constructor(params, logger) {
        super(params, logger, "aws_s3");
    }
    static generate_id(params) {
        return JSON.stringify({accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
    }
    update_settings(params) {
        this.bucket = params.bucket;
        if (this.id() !== this.constructor.generate_id(params)) this.S3 = new S3({accessKeyId: params.access_key, secretAccessKey: params.secret, region: params.region});
        super.update_settings(params);
    }
    read(filename, encoding = 'utf-8') {
        return this.queue.run(() => this.S3.getObject({Bucket: this.bucket, Key: filename}).promise().then(data => this.constructor.get_data(data.Body, encoding)));
    }
    stat(filename) {
        return this.queue.run(() => this.S3.headObject({Bucket: this.bucket, Key: filename}).promise()).then(data => ({
            size: data.ContentLength,
            mtime: data.LastModified,
            isDirectory: () => filename.endsWith('/')
        }));
    }
    createReadStream(source) {
        return this.queue.run(() => this.S3.getObject({Bucket: this.bucket, Key: source}).createReadStream());
    }
    mkdir(dir) {
        return this.queue.run(() => this.S3.createBucket({Bucket: this.bucket}).promise())
    }
    write(target, contents = '') {
        return this.queue.run(() => this.S3.putObject({Bucket: this.bucket, Key: target, Body: contents}).promise())
    }
    copy(source, target, streams, size, params) {
        return this.queue.run(() => new Promise((resolve, reject) => {
            let partSize = params.partSize || 50 * 1024 * 1024;
            while (size / partSize > 10000) partSize *= 2;
            let options = params.options || {partSize: partSize, queueSize: params.concurrency || 8};
            let result = this.S3.upload({Bucket: this.bucket, Key: target, Body: streams.passThrough, ContentType: mime.lookup(source)}, options, (err,data) => {
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
        }))
    }
    remove(target) {
        return this.queue.run(() => this.S3.deleteObject({Bucket: this.bucket, Key: target}).promise());
    }
    move(source, target, params) {
        return this.queue.run(() => this.S3.copyObject({Bucket: this.bucket, Key: target, CopySource: encodeURI((params.origin_bucket || this.bucket) + "/" + source)}).promise()
            .then(()=> this.S3.deleteObject({Bucket: (params.origin_bucket || this.bucket), Key: source}, ).promise()))
    }
    walk(dirname, ignored, token) {
        return this.queue.run(() => this.S3.listObjectsV2({Bucket: this.bucket, Prefix: this.constructor.normalize_path(dirname), ContinuationToken: token}).promise())
            .then(data => {
                let list = data.Contents || [];
                for (let i = 0; i < list.length; i++) {
                    let {Key, Size} = list[i];
                    if (Key.match(ignored)) return;
                    if (!Key.endsWith('/')) {
                        if (!this.fileObjects[Key] || (this.fileObjects[Key] && Size !== this.fileObjects[Key].size)) this.add(Key, {size: Size, mtime: list[i].LastModified, isDirectory: () => Key.endsWith('/')});
                        this.fileObjects[Key] = {last_seen: this.now, size: Size};
                    }
                }

                if (data.IsTruncated) return this.walk(dirname, ignored, data.NextContinuationToken);
            })
            .catch(err => {
                this.logger.error("AWS S3 walk failed with dirname: ", dirname, err);
                super.error(err);
            });
    }
    static normalize_path(dirname, is_filename) {
        return dirname.replace(/[\\\/]+/g, "/").replace(/\/+$/, "").concat(is_filename ? "" : "/").replace(/^\/+/, "")
    }
    filename(dirname, uri) {
        return uri.slice(dirname.length);
    }
    path(dirname, filename) {
        return this.constructor.normalize_path(dirname) + filename;
    }
}
