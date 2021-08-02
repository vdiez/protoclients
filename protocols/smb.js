const Client = require('@marsaud/smb2');
const path = require('path').posix;
const Stream = require('stream');
const Base = require('../base');
const publish = require('../default_publish');

class SMB extends Base {
    static parameters = {
        domain: {
            text: true
        },
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
        share: {
            text: true
        },
        username: {
            text: true
        }
    };

    constructor(params, logger) {
        super('smb', params, logger);
    }

    static generate_id(params) {
        return JSON.stringify({
            protocol: 'smb',
            share: `\\\\${params.host}\\${params.share}`,
            username: params.username,
            password: params.password,
            port: params.port,
            domain: params.domain
        });
    }

    connect(slot) {
        if (!this.connections[slot]) {
            this.connections[slot] = new Client({
                share: `\\\\${this.params.host}\\${this.params.share}`,
                username: this.params.username,
                password: this.params.password,
                port: this.params.port,
                domain: this.params.domain,
                autoCloseTimeout: 0
            });
            this.logger.info(`SMB (slot ${slot}) connection established with ${this.params.host}`);
        }
        return Promise.resolve(this.connections[slot]);
    }

    disconnect(slot) {
        if (!this.connections[slot]) {
            return;
        }
        this.connections[slot].disconnect();
        this.logger.info(`SMB (slot ${slot}) connection closed with ${this.params.host}`);
    }

    createReadStream(source, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) create read stream from: `, source);
            connection.createReadStream(source.replace(/\//g, '\\'), options, (err, stream) => {
                if (err) {
                    reject(err);
                } else {
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('end', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    resolve(stream);
                }
            });
        }), control_release);
    }

    createWriteStream(target, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) create write stream to: `, target);
            connection.createWriteStream(target.replace(/\//g, '\\'), options, (err, stream) => {
                if (err) reject(err);
                else {
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('finish', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    resolve(stream);
                }
            });
        }), control_release);
    }

    mkdir(dir) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) mkdir: `, dir);
            connection.mkdir(dir.replace(/\//g, '\\'), err => {
                if (err && err.code !== 'STATUS_OBJECT_NAME_COLLISION') {
                    reject(err);
                } else {
                    resolve();
                }
            });
        }));
    }

    read(filename, params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) download from: `, filename);
            if (params.start || params.end) {
                connection.createReadStream(filename.replace(/\//g, '\\'), params, (err, stream) => {
                    if (err) {
                        reject(err);
                    } else {
                        Base.get_data(stream, params.encoding).then(resolve).catch(reject);
                    }
                });
            } else {
                connection.readFile(filename.replace(/\//g, '\\'), {encoding: params.encoding}, (err, contents) => (err ? reject(err) : resolve(contents)));
            }
        }));
    }

    write(target, contents = Buffer.allocUnsafe(0), params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) upload to: `, target);
            if (params.start || params.end) {
                connection.createWriteStream(target.replace(/\//g, '\\'), params, (err, stream) => {
                    if (err) {
                        reject(err);
                    } else {
                        new Stream.Readable({
                            read() {
                                this.push(contents, params.encoding);
                                this.push(null); // https://nodejs.org/api/stream.html#stream_readable_push_chunk_encoding
                            }
                        }).pipe(stream);
                        stream.on('error', reject);
                        stream.on('end', resolve);
                        stream.on('close', resolve);
                    }
                });
            } else {
                connection.writeFile(target, contents, {encoding: params.encoding}, err => (err ? reject(err) : resolve()));
            }
        }));
    }

    stat(filename) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) stat: `, filename);
            connection.stat(filename.replace(/\//g, '\\'), (err, stat) => (err ? reject(err) : resolve(stat)));
        }));
    }

    copy(source, target, streams, size, params) {
        if (!streams.readStream) {
            throw {
                message: `local copy not implemented for ${this.protocol}`,
                not_implemented: 1
            };
        }
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) upload stream to: `, target);
            connection.createWriteStream(target.replace(/\//g, '\\'), (err, stream) => {
                streams.writeStream = stream;
                streams.writeStream.on('finish', resolve);
                streams.writeStream.on('error', reject);

                streams.passThrough.on('error', reject);
                streams.passThrough.pipe(streams.writeStream);

                streams.readStream.on('error', reject);
                streams.readStream.pipe(streams.passThrough);

                publish(streams.readStream, size, params.publish);
            });
        }));
    }

    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) remove: `, target);
            connection.unlink(target.replace(/\//g, '\\'), err => (err ? reject(err) : resolve()));
        }));
    }

    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) move: `, source, target);
            connection.rename(source.replace(/\//g, '\\'), target.replace(/\//g, '\\'), {replace: true}, err => (err ? reject(err) : resolve()));
        }));
    }

    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) list: `, dirname);
            connection.readdir(dirname.replace(/\//g, '\\'), {stats: true}, (err, list) => (err ? reject(err) : resolve(list)));
        }));
    }

    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.list(dirname).then(list => list.reduce((p, file) => p.then(() => {
            const filename = path.join(dirname, file.name);
            if (filename.match(ignored)) {
                return;
            }
            if (file.isDirectory()) {
                pending_paths.push(filename);
            } else {
                on_file(filename, {
                    size: file.size,
                    mtime: file.mtime,
                    isDirectory: () => false
                });
            }
        }).catch(on_error), Promise.resolve())).then(() => (pending_paths.length ? this.walk({
            dirname: pending_paths.shift(),
            ignored,
            on_file,
            on_error,
            pending_paths
        }) : null));
    }
}

module.exports = SMB;
