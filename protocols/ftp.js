const Client = require('@icetee/ftp');
const path = require('path');
const Stream = require('stream');
const base = require('../base');

module.exports = class extends base {
    static parameters = {
        ...base.parameters,
        parallel: {type: 'number', max: 50},
        host: {type: 'text'},
        port: {type: 'number', min: 0, max: 65535},
        secure: {type: 'boolean'},
        username: {type: 'text'},
        password: {type: 'secret'}
    };

    constructor(params, logger) {
        super(params, logger, 'ftp');
    }

    static generate_id(params) {
        return JSON.stringify({protocol: 'ftp', host: params.host, user: params.username, password: params.password, port: params.port, secure: params.secure});
    }

    connect(slot) {
        if (this.connections[slot]) return this.connections[slot];
        return new Promise((resolve, reject) => {
            const client = new Client();
            client
                .on('ready', () => {
                    this.connections[slot] = client;
                    this.logger.info(`FTP (slot ${slot}) connection established with ${this.params.host}`);
                    resolve(this.connections[slot]);
                })
                .on('error', err => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection to host ${this.params.host} error: ${err}`);
                })
                .on('end', () => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection ended to host ${this.params.host}`);
                })
                .on('close', had_error => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection lost to host ${this.params.host}. Due to error: ${had_error}`);
                })
                .connect({host: this.params.host, user: this.params.username, password: this.params.password, port: this.params.port, secure: this.params.secure, secureOptions: {rejectUnauthorized: false}});
        });
    }

    disconnect(slot) {
        if (!this.connections[slot]) return;
        this.connections[slot].end();
        this.logger.info(`FTP (slot ${slot}) connection closed with ${this.params.host}`);
    }

    createReadStream(source, params = {}) {
        return this.wrapper((connection, slot, slot_control) => Promise.resolve()
            .then(() => {
                if (params.start) {
                    return new Promise((resolve, reject) => {
                        this.logger.debug(`FTP (slot ${slot}) restart readStream from(bytes): `, source, params.start);
                        connection.restart(params.start, err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    });
                }
            })
            .then(() => new Promise((resolve, reject) => {
                this.logger.debug(`FTP (slot ${slot}) create read stream from: `, source);
                connection.get(source, (err, stream) => {
                    if (err) reject(err);
                    else {
                        slot_control.keep_busy = true;
                        stream.on('error', slot_control.release_slot);
                        stream.on('end', slot_control.release_slot);
                        stream.on('close', slot_control.release_slot);
                        if (params.end) {
                            let missing = params.end - (params.start || 0) + 1;
                            let finished = false;
                            const limiter = new Stream.Transform({
                                transform(chunk, encoding, callback) {
                                    const length = Buffer.byteLength(chunk);
                                    if (finished) return;

                                    if (missing - length <= 0) {
                                        stream.destroy();
                                        finished = true;
                                        callback(null, missing === length ? chunk : chunk.slice(0, missing));
                                    }
                                    else {
                                        missing -= length;
                                        callback(null, chunk);
                                    }
                                }
                            });
                            stream.pipe(limiter);
                            limiter.on('error', () => stream.destroy(err));
                            limiter.on('end', () => stream.destroy());
                            limiter.on('close', () => stream.destroy());
                            resolve(limiter);
                        }
                        else resolve(stream);
                    }
                });
            })), true);
    }

    createWriteStream(target, params = {}) {
        return this.wrapper((connection, slot, slot_control) => Promise.resolve()
            .then(() => {
                if (params.start) {
                    return new Promise((resolve, reject) => {
                        this.logger.debug(`FTP (slot ${slot}) restart writeStream to(bytes): `, target, params.start);
                        connection.restart(params.start, err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    });
                }
            })
            .then(() => new Promise((resolve, reject) => {
                this.logger.debug(`FTP (slot ${slot}) create write stream to: `, target);
                slot_control.keep_busy = true;
                const size = params?.size || 0;

                let complete_transfer = () => {};
                const complete_promise = new Promise(resolve_complete_transfer => {
                    complete_transfer = resolve_complete_transfer;
                });
                let transferred = 0;
                const stream = new Stream.Transform({
                    transform(chunk, encoding, callback) {
                        transferred += Buffer.byteLength(chunk);
                        if (params.hasOwnProperty('size') && transferred >= size) {
                            this.push(chunk);
                            this.push(null);//signal end of read stream
                            complete_promise.then(() => {callback();});//wait for upload to signal end of write stream
                        }
                        else callback(null, chunk);
                    }
                });
                connection.put(stream, target, err => {
                    complete_transfer();
                    if (err) reject(err);
                    slot_control.release_slot();
                });
                resolve(stream);
            })), true);
    }

    mkdir(dir) {
        if (dir === '.' || dir === '/' || dir === '') return;
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) mkdir: `, dir);
            connection.mkdir(dir, true, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }

    list_uri(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) list/stat: `, dirname);
            connection.list(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            });
        }));
    }

    read(filename, params = {}) {
        return this.createReadStream(filename, params).then(stream => this.constructor.get_data(stream, params.encoding));
    }

    stat(file) {
        return this.list_uri(file)
            .then(list => {
                if (!list || !list.length) return null;
                if (list?.length === 1 && path.posix.basename(list[0]?.name) === path.posix.basename(file)) return {size: list[0].size, mtime: list[0].date, isDirectory: () => false};
                return {size: 0, mtime: new Date(), isDirectory: () => true};
            });
    }

    write(target, contents = Buffer.alloc(0), params = {}) {
        return this.wrapper((connection, slot) => Promise.resolve()
            .then(() => {
                if (params.start) {
                    return new Promise((resolve, reject) => {
                        this.logger.debug(`FTP (slot ${slot}) restart put to(bytes): `, target, params.start);
                        connection.restart(params.start, err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    });
                }
            })
            .then(() => new Promise((resolve, reject) => {
                this.logger.debug(`FTP (slot ${slot}) upload to: `, target);
                connection.put(contents, target, err => {
                    if (err) reject(err);
                    else resolve();
                });
            })));
    }

    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) remove: `, target);
            connection.delete(target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }

    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) move: `, source, target);
            connection.rename(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }

    list(dirname) {
        return this.list_uri(dirname)
            .then(list => {
                for (let i = 0; i < list.length; i++) {
                    list[i].isDirectory = () => list[i].type === 'd';
                    list[i].mtime = list[i].date;
                }
                return list;
            });
    }

    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.list_uri(dirname)
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    const filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.type === 'd') pending_paths.push(filename);
                    else on_file(filename, {size: file.size, mtime: file.date, isDirectory: () => false});
                })
                .catch(on_error), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk({dirname: pending_paths.shift(), ignored, on_file, on_error, pending_paths});
            });
    }
};
