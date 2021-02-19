let Client = require('@marsaud/smb2');
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = {
        parallel: {number: true},
        host: {text: true},
        share: {text: true},
        domain: {text: true},
        port: {number: true},
        username: {text: true},
        password: {secret: true},
        polling: {boolean: true},
        polling_interval: {number: true}
    };
    constructor(params, logger) {
        super(params, logger, "smb");
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'smb', share: "\\\\" + params.host + "\\" + params.share, username: params.username, password: params.password, port: params.port, domain: params.domain});
    }
    connect(slot) {
        if (!this.connections[slot]) {
            this.connections[slot] = new Client({share: "\\\\" + this.params.host + "\\" + this.params.share, username: this.params.username, password: this.params.password, port: this.params.port, domain: this.params.domain, autoCloseTimeout: 0});
            this.logger.info("SMB (slot " + slot + ") connection established with " + this.params.host);
        }
        return this.connections[slot];
    }
    disconnect(slot) {
        if (!this.connections[slot]) return;
        this.connections[slot].disconnect()
        this.logger.info("SMB (slot " + slot + ") connection closed with " + this.params.host);
    }
    createReadStream(source, options) {
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") create stream from: ", source);
            connection.createReadStream(source.replace(/\//g, "\\"), options, (err, stream) => {
                if (err) reject(err);
                else {
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('end', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    resolve(stream);
                }
            })
        }), true);
    }
    mkdir(dir) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") mkdir: ", dir);
            connection.mkdir(dir.replace(/\//g, "\\"), err => {
                if (err && err.code !== 'STATUS_OBJECT_NAME_COLLISION') reject(err);
                else resolve();
            })
        }));
    }
    read(filename, params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") download from: ", filename);
            connection.readFile(filename.replace(/\//g, "\\"), {encoding: params.encoding}, (err, contents) => {
                if (err) reject(err);
                else resolve(contents);
            });
        }));
    }
    write(target, contents = new Buffer(0), params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") upload to: ", target);
            connection.writeFile(target, contents, {encoding: params.encoding}, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    stat(filename) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") stat: ", filename);
            connection.stat(filename.replace(/\//g, "\\"), (err, stat) => {
                if (err) reject(err);
                else resolve(stat);
            });
        }));
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: "local copy not implemented for " + this.protocol, not_implemented: 1}
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") upload stream to: ", target);
            connection.createWriteStream(target.replace(/\//g, "\\"), (err, stream) => {
                streams.writeStream = stream;
                streams.writeStream.on('finish', resolve);
                streams.writeStream.on('error', reject);
                streams.readStream.on('error', reject);
                streams.passThrough.on('error', reject);
                streams.readStream.pipe(streams.passThrough);
                streams.passThrough.pipe(streams.writeStream);
                publish(streams.readStream, size, params.publish);
            });
        }));
    }
    remove(target)  {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") remove: ", target);
            connection.unlink(target.replace(/\//g, "\\"), err => {
                if (err) reject(err);
                else resolve();
            })
        }));
    }
    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") move: ", source, target);
            connection.rename(source.replace(/\//g, "\\"), target.replace(/\//g, "\\"), {replace: true}, err => {
                if (err) reject(err);
                else resolve();
            })
        }));
    }
    walk(dirname, ignored, pending_paths = []) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SMB (slot " + slot + ") list: ", dirname);
            connection.readdir(dirname.replace(/\//g, "\\"), {stats: true}, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }))
        .then(list => list.reduce((p, file) => p
            .then(() => {
                let filename = path.posix.join(dirname, file.name);
                if (filename.match(ignored)) return;
                if (file.isDirectory()) pending_paths.push(filename);
                {
                    if (!this.fileObjects[filename] || (this.fileObjects[filename] && file.size !== this.fileObjects[filename].size)) {
                        this.logger.info("SMB walk adding: ", filename);
                        this.on_file_added(filename, {size: file.size, mtime: file.mtime, isDirectory: () => false});
                    }
                    this.fileObjects[filename] = {last_seen: this.now, size: file.size};
                }
            })
            .catch(err => {
                this.logger.error("SMB walk for '" + file + "' failed: ", err);
                this.on_error(err);
            }), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk(pending_paths.shift(), ignored, pending_paths);
            })
    }
}
