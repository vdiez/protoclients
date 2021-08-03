const Client = require('ftp');
const path = require('path').posix;
const Stream = require('stream');
const Base = require('../base');
const publish = require('../default_publish');
const {rootDirnames} = require('../config');

class FTP extends Base {
    static accept_ranges = true;

    static parameters = {
        host: {
            text: true
        },
        parallel: {
            number: true
        },
        password: {
            secret: true
        },
        polling: {
            boolean: true
        },
        polling_interval: {
            number: true
        },
        port: {
            number: true
        },
        secure: {
            boolean: true
        },
        username: {
            text: true
        }
    };

    constructor(params, logger) {
        super('ftp', params, logger);
    }

    static generate_id(params) {
        return JSON.stringify({
            protocol: 'ftp',
            host: params.host,
            user: params.username,
            password: params.password,
            port: params.port,
            secure: params.secure
        });
    }

    connect(slot) {
        return new Promise((resolve, reject) => {
            if (!this.connections?.[slot]) {
                const client = new Client();
                this.connections[slot] = client;
                client.on('ready', () => {
                    this.logger.info(`FTP (slot ${slot}) connection established with ${this.params.host}`);
                    resolve(client);
                });
                client.on('error', err => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection to host ${this.params.host} error: ${err}`);
                });
                client.on('end', () => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection ended to host ${this.params.host}`);
                });
                client.on('close', had_error => {
                    this.connections[slot] = null;
                    reject(`FTP (slot ${slot}) connection lost to host ${this.params.host}. Due to error: ${had_error}`);
                });
                client.connect({
                    host: this.params.host,
                    user: this.params.username,
                    password: this.params.password,
                    port: this.params.port,
                    secure: this.params.secure,
                    secureOptions: {
                        rejectUnauthorized: false
                    }
                });
            }
            return resolve(this.connections[slot]);
        });
    }

    disconnect(slot) {
        if (!this.connections[slot]) {
            return;
        }
        this.connections[slot].end();
        this.logger.info(`FTP (slot ${slot}) connection closed with ${this.params.host}`);
    }

    createReadStream(source, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => {
            const restartPromise = new Promise((resolve, reject) => {
                if (!options.start) {
                    return resolve();
                }
                this.logger.debug(`FTP (slot ${slot}) restart readStream from(bytes): `, source, options.start);
                return connection.restart(options.start, err => (err ? reject(err) : resolve()));
            });
            const getPromise = new Promise((resolve, reject) => {
                connection.get(source, (err, stream) => {
                    if (err) {
                        return reject(err);
                    }
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('end', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    if (options.end) {
                        let missing = options.end - (options.start || 0) + 1;
                        let finished = false;
                        const limiter = new Stream.Transform({
                            transform(chunk, encoding, callback) {
                                const length = Buffer.byteLength(chunk);
                                if (finished) {
                                    return;
                                }
                                if (missing - length <= 0) {
                                    stream.destroy();
                                    finished = true;
                                    callback(null, missing === length ? chunk : chunk.slice(0, missing));
                                } else {
                                    missing -= length;
                                    callback(null, chunk);
                                }
                            }
                        });
                        stream.pipe(limiter);
                        limiter.on('error', () => stream.destroy(err));
                        limiter.on('end', () => stream.destroy());
                        limiter.on('close', () => stream.destroy());
                        return resolve(limiter);
                    }
                    return resolve(stream);
                });
            });

            return restartPromise.then(() => {
                this.logger.debug(`FTP (slot ${slot}) create read stream from: `, source);
                return getPromise;
            });
        }, control_release);
    }

    createWriteStream(target, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => {
            const restartPromise = new Promise((resolve, reject) => {
                if (!options.start) {
                    return resolve();
                }
                this.logger.debug(`FTP (slot ${slot}) restart writeStream from(bytes): `, target, options.start);
                return connection.restart(options.start, err => (err ? reject(err) : resolve()));
            });
            const putPromise = new Promise((resolve, reject) => {
                slot_control.keep_busy = true;
                const stream = new Stream.PassThrough();
                connection.put(stream, target, err => {
                    if (err) {
                        reject(err);
                    }
                    slot_control.release_slot();
                });
                resolve(stream);
            });
            return restartPromise.then(() => {
                this.logger.debug(`FTP (slot ${slot}) create write stream to: `, target);
                return putPromise;
            });
        }, control_release);
    }

    mkdir(dir) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            if (rootDirnames.includes(dir)) {
                resolve();
            }
            this.logger.debug(`FTP (slot ${slot}) mkdir: `, dir);
            connection.mkdir(dir, true, err => (err ? reject(err) : resolve()));
        }));
    }

    list_uri(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) list/stat: `, dirname);
            connection.list(dirname, (err, list) => (err ? reject(err) : resolve(list)));
        }));
    }

    read(filename, params = {}) {
        const control_release = true;
        return this.wrapper((connection, slot) => {
            const restartPromise = new Promise((resolve, reject) => {
                if (!params.start) {
                    return resolve();
                }
                this.logger.debug(`FTP (slot ${slot}) restart get from(bytes): `, filename, params.start);
                return connection.restart(params.start, err => (err ? reject(err) : resolve()));
            });
            const getPromise = new Promise((resolve, reject) => {
                connection.get(filename, (err, stream) => {
                    if (err) {
                        reject(err);
                    } else if (params.end) {
                        let missing = params.end - (params.start || 0) + 1;
                        const chunks = [];
                        stream.on('error', reject);
                        stream.on('end', () => resolve(Buffer.concat(chunks, params.end - params.start)));
                        stream.on('close', () => resolve(Buffer.concat(chunks, params.end - params.start)));
                        stream.on('data', chunk => {
                            const length = Buffer.byteLength(chunk);
                            if (missing - length < 0) {
                                stream.destroy();
                                chunks.push(chunk.slice(0, missing));
                            } else {
                                missing -= length;
                                chunks.push(chunk);
                            }
                        });
                    } else {
                        Base.get_data(stream, params.encoding).then(resolve).catch(reject);
                    }
                });
            });
            return restartPromise.then(() => {
                this.logger.debug(`FTP (slot ${slot}) download from: `, filename);
                return getPromise;
            });
        }, control_release);
    }

    stat(file) {
        return this.list_uri(file).then(list => {
            if (!list || !list.length) return null;
            const isFile = list.length === 1 && path.basename(list[0]?.name) === path.basename(file);
            if (isFile) {
                return {
                    size: list[0].size,
                    mtime: list[0].date,
                    isDirectory: () => false
                };
            }
            return {
                size: 0,
                mtime: new Date(),
                isDirectory: () => true
            };
        });
    }

    write(target, contents = Buffer.allocUnsafe(0), params) {
        return this.wrapper((connection, slot) => {
            const restartPromise = new Promise((resolve, reject) => {
                if (!params.start) {
                    return resolve();
                }
                this.logger.debug(`FTP (slot ${slot}) restart put from(bytes): `, target, params.start);
                return connection.restart(params.start, err => (err ? reject(err) : resolve()));
            });
            const putPromise = new Promise((resolve, reject) => {
                connection.put(contents, target, err => (err ? reject(err) : resolve()));
            });
            return restartPromise.then(() => {
                this.logger.debug(`FTP (slot ${slot}) download from: `, target);
                return putPromise;
            });
        });
    }

    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: `local copy not implemented for ${this.protocol}`, not_implemented: 1};
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) upload stream to: `, target);
            streams.passThrough.on('error', reject);
            streams.readStream.on('error', reject);
            streams.readStream.pipe(streams.passThrough);
            connection.put(streams.passThrough, target, err => (err ? reject(err) : resolve()));
            publish(streams.readStream, size, params.publish);
        }));
    }

    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) remove: `, target);
            connection.delete(target, err => (err ? reject(err) : resolve()));
        }));
    }

    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) move: `, source, target);
            connection.rename(source, target, err => (err ? reject(err) : resolve()));
        }));
    }

    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`FTP (slot ${slot}) list/stat: `, dirname);
            connection.list(dirname, (err, list) => (err ? reject(err) : resolve(list)));
        })).then(list => list.map(stat => {
            stat.isDirectory = () => stat.type === 'd'; // Why is it a function ?
            stat.mtime = stat.date;
            return stat;
        }));
    }

    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.list_uri(dirname).then(list => {
            return list.reduce((p, file) => {
                return p.then(() => {
                    const filename = path.join(dirname, file.name);
                    if (filename.match(ignored)) {
                        return;
                    }
                    if (file.type === 'd') {
                        pending_paths.push(filename);
                    } else {
                        on_file(filename, {
                            size: file.size,
                            mtime: file.date,
                            isDirectory: () => false
                        });
                    }
                }).catch(on_error);
            }, Promise.resolve());
        }).then(() => {
            return pending_paths.length ? this.walk({
                dirname: pending_paths.shift(),
                ignored,
                on_file,
                on_error,
                pending_paths
            }) : null;
        });
    }
}

module.exports = FTP;
