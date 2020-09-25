let Client = require('@marsaud/smb2');
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = ["polling", "polling_interval", "host", "port", "username", "password"];
    constructor(params, logger) {
        super(params, logger, "smb");
        this.connection = null;
    }
    static generate_id(params) {
        return JSON.stringify({share: "\\\\" + params.host + "\\" + params.share, username: params.username, password: params.password, port: params.port, domain: params.domain});
    }
    update_settings(params) {
        params.parallel_parsers = 1;
        if (this.id() !== this.constructor.generate_id(params)) this.disconnect();
        super.update_settings(params);
    }
    connect(force) {
        if (this.connection && !force) return;
        this.connection = new Client({share: "\\\\" + this.params.host + "\\" + this.params.share, username: this.params.username, password: this.params.password, port: this.params.port, domain: this.params.domain, autoCloseTimeout: 0});
    }
    disconnect() {
        return this.queue.run(() => this.connection.disconnect());
    }
    createReadStream(source) {
        return this.wrapper(() => this.connection.createReadStream(source.replace(/\//g, "\\")));
    }
    mkdir(dir) {
        return this.wrapper(() => this.connection.mkdir(dir.replace(/\//g, "\\"))
            .catch(err => {
                if (err && err.code === 'STATUS_OBJECT_NAME_COLLISION') return;
                throw err;
            }));
    }
    read(filename, encoding = 'utf8') {
        return this.wrapper(() => this.connection.readFile(filename.replace(/\//g, "\\"), {encoding: encoding}));
    }
    write(target, contents = '', encoding = 'utf8') {
        return this.wrapper(() => this.connection.writeFile(target, contents), {encoding: encoding});
    }
    stat(filename) {
        return this.wrapper(() => this.connection.stat(filename.replace(/\//g, "\\")));
    }
    copy(source, target, streams, size, params) {
        return this.wrapper(() => this.connection.createWriteStream(target.replace(/\//g, "\\"))
            .then(stream => new Promise((resolve, reject) => {
                streams.writeStream = stream;
                streams.writeStream.on('finish', () => {resolve()});
                streams.writeStream.on('error', err => reject(err));
                streams.readStream.on('error', err => reject(err));
                streams.passThrough.on('error', err => reject(err));
                streams.readStream.pipe(streams.passThrough);
                streams.passThrough.pipe(streams.writeStream);
                publish(streams.readStream, size, params.publish);
            })))
    }
    remove(target)  {
        return this.wrapper(() => this.connection.unlink(target.replace(/\//g, "\\")));
    }
    move(source, target) {
        return this.wrapper(() => this.connection.rename(source.replace(/\//g, "\\"), target.replace(/\//g, "\\"), {replace: true}));
    }
    walk(dirname, ignored) {
        return this.wrapper(() => this.connection.readdir(dirname.replace(/\//g, "\\"), {stats: true}))
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    let filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.isDirectory()) return this.walk(filename, ignored);
                    if (!this.fileObjects[filename] || (this.fileObjects[filename] && file.size !== this.fileObjects[filename].size)) this.add(filename, {size: file.size, mtime: file.mtime, isDirectory: () => false});
                    this.fileObjects[filename] = {last_seen: this.now, size: file.size};
                })
                .catch(err => {
                    this.logger.error("SMB walk for '" + file + "' failed: ", err);
                    this.on_error(err);
                }), Promise.resolve()))
    }
}
