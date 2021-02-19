let Client = require('ftp');
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = {
        parallel: {number: true},
        host: {text: true},
        port: {number: true},
        secure: {boolean: true},
        username: {text: true},
        password: {secret: true},
        polling: {boolean: true},
        polling_interval: {number: true}
    };
    static accept_ranges = false;

    constructor(params, logger) {
        super(params, logger, "ftp");
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'ftp', host: params.host, user: params.username, password: params.password, port: params.port, secure: params.secure});
    }
    connect(slot) {
        if (this.connections[slot]) return this.connections[slot];
        return new Promise((resolve, reject) => {
            this.connections[slot] = new Client();
            this.connections[slot]
                .on('ready', () => {
                    this.logger.info("FTP (slot " + slot + ") connection established with " + this.params.host);
                    resolve(this.connections[slot]);
                })
                .on('error', err => {
                    this.connections[slot] = null;
                    reject("FTP (slot " + slot + ") connection to host " + this.params.host + " error: " + err);
                })
                .on('end', () => {
                    this.connections[slot] = null;
                    reject("FTP (slot " + slot + ") connection ended to host " + this.params.host);
                })
                .on('close', had_error => {
                    this.connections[slot] = null;
                    reject("FTP (slot " + slot + ") connection lost to host " + this.params.host + ". Due to error: " + had_error);
                })
                .connect({host: this.params.host, user: this.params.username, password: this.params.password, port: this.params.port, secure: this.params.secure, secureOptions: {rejectUnauthorized: false}});
        });
    }
    disconnect(slot) {
        if (!this.connections[slot]) return;
        this.connections[slot].end();
        this.logger.info("FTP (slot " + slot + ") connection closed with " + this.params.host);
    }
    createReadStream(source) {
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") create stream from: ", source);
            connection.get(source, (err, stream) => {
                if (err) reject(err);
                else {
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('end', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    resolve(stream);
                }
            });
        }), true);
    }
    mkdir(dir) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            if (dir === "." || dir === "/" || dir === "") resolve();
            this.logger.debug("FTP (slot " + slot + ") mkdir: ", dir);
            connection.mkdir(dir, true, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") list/stat: ", dirname);
            connection.list(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }));
    }
    read(filename, params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") download from: ", filename);
            connection.get(filename, (err, stream) => {
                if (err) reject(err);
                else this.constructor.get_data(stream, params.encoding).then(data => resolve(data)).catch(err => reject(err));
            })
        }));
    }
    stat(file) {
        return this.list(file)
            .then(list => {
                if (list.length === 1 && list[0]?.name === file) return {size: list[0].size, mtime: list[0].date, isDirectory: () => false};
                else return {size: 0, mtime: new Date(), isDirectory: () => true};
            });
    }
    write(target, contents = new Buffer(0)) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") upload to: ", target);
            connection.put(contents, target, err => {
                if (err) reject(err);
                else resolve();
            })
        }));
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: "local copy not implemented for " + this.protocol, not_implemented: 1}
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") upload stream to: ", target);
            streams.readStream.on('error', reject);
            streams.passThrough.on('error', reject);
            streams.readStream.pipe(streams.passThrough);
            connection.put(streams.passThrough, target, err => {
                if (err) reject(err);
                else resolve();
            });
            publish(streams.readStream, size, params.publish);
        }));
    }
    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") remove: ", target);
            connection.delete(target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") move: ", source, target);
            connection.rename(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    walk(dirname, ignored, pending_paths = []) {
        return this.list(dirname)
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    let filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.type === 'd') pending_paths.push(filename);
                    else {
                        if (!this.fileObjects[filename] || (this.fileObjects[filename] && file.size !== this.fileObjects[filename].size)) {
                            this.logger.info("FTP walk adding: ", filename);
                            this.on_file_added(filename, {size: file.size, mtime: file.date, isDirectory: () => false})
                        }
                        this.fileObjects[filename] = {last_seen: this.now, size: file.size};
                    }
                })
                .catch(err => {
                    this.logger.error("FTP walk for '" + file + "' failed: ", err);
                    this.on_error(err);
                }), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk(pending_paths.shift(), ignored, pending_paths);
            })
    }
}
