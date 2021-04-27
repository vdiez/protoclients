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
    createReadStream(source, options) {
        return this.wrapper((connection, slot, slot_control) => Promise.resolve()
            .then(() => {
                if (options.start) {
                    return new Promise((resolve, reject) => {
                        this.logger.debug("FTP (slot " + slot + ") restart get from(bytes): ", source, options.start);
                        connection.restart(options.start, err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    })
                }
            })
            .then(() => new Promise((resolve, reject) => {
                this.logger.debug("FTP (slot " + slot + ") create read stream from: ", source);
                connection.get(source, (err, stream) => {
                    if (err) reject(err);
                    else {
                        slot_control.keep_busy = true;
                        stream.on('error', slot_control.release_slot);
                        stream.on('end', slot_control.release_slot);
                        stream.on('close', slot_control.release_slot);
                        if (options.end) {
                            let missing = params.end - (params.start || 0);
                            let limiter = new (require('stream')).Transform({
                                transform(chunk, encoding, callback) {
                                    let length = Buffer.byteLength(chunk);

                                    if (missing - length < 0) {
                                        stream.destroy();
                                        callback(null, chunk.slice(0, missing));
                                    }
                                    else {
                                        missing -= length;
                                        callback(null, chunk);
                                    }
                                }
                            });
                            stream.pipe(limiter);
                            resolve(limiter);
                        }
                        else resolve(stream);
                    }
                });
            })), true);
    }
    createWriteStream(target) {
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") create write stream to: ", target);
            slot_control.keep_busy = true;
            let stream = {passThrough: new (require('stream')).PassThrough()};
            connection.put(stream, target, err => {
                if (err) reject(err);
                slot_control.release_slot();
            });
            resolve(stream);
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
    list_uri(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") list/stat: ", dirname);
            connection.list(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }));
    }
    read(filename, params = {}) {
        return this.wrapper((connection, slot) => Promise.resolve()
            .then(() => {
                if (params.start) {
                    return new Promise((resolve, reject) => {
                        this.logger.debug("FTP (slot " + slot + ") restart get from(bytes): ", filename, params.start);
                        connection.restart(params.start, err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    })
                }
            })
            .then(() => new Promise((resolve, reject) => {
                this.logger.debug("FTP (slot " + slot + ") download from: ", filename);
                connection.get(filename, (err, stream) => {
                    if (err) reject(err);
                    else {
                        if (params.end) {
                            let missing = params.end - (params.start || 0);
                            let chunks = [];
                            stream.on('error', reject);
                            stream.on('end', () => resolve(Buffer.concat(chunks, params.end - params.start)));
                            stream.on('close', () => resolve(Buffer.concat(chunks, params.end - params.start)));
                            stream.on('data', chunk => {
                                let length = Buffer.byteLength(chunk);

                                if (missing - length < 0) {
                                    stream.destroy();
                                    chunks.push(chunk.slice(0, missing));
                                }
                                else {
                                    missing -= length;
                                    chunks.push(chunk);
                                }
                            });
                        }
                        else this.constructor.get_data(stream, params.encoding).then(data => resolve(data)).catch(err => reject(err));
                    }
                })
            })
        ));
    }
    stat(file) {
        return this.list_uri(file)
            .then(list => {
                if (list?.length === 1 && list[0]?.name === file) return {size: list[0].size, mtime: list[0].date, isDirectory: () => false};
                return {size: 0, mtime: new Date(), isDirectory: () => true};
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
    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("FTP (slot " + slot + ") list/stat: ", dirname);
            connection.list(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }))
            .then(list => {
                for (let i = 0; i < list.length; i++) {
                    list[i].isDirectory = () => list[i].type === "d";
                    list[i].mtime = list[i].date;
                }
                return list;
            });
    }
    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.list_uri(dirname)
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    let filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.type === 'd') pending_paths.push(filename);
                    else on_file(filename, {size: file.size, mtime: file.date, isDirectory: () => false})
                })
                .catch(on_error), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk({dirname: pending_paths.shift(), ignored, on_file, on_error, pending_paths});
            })
    }
}
