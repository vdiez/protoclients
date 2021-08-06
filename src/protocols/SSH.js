const Client = require('ssh2').Client;
const path = require('path').posix;
const Stream = require('stream');
const Base = require('./Base');
const publish = require('../default_publish');
const {rootDirnames} = require('../../config');

class SSH extends Base {
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
        username: {
            text: true
        }
    };

    constructor(params, logger) {
        super('ssh', params, logger);
        this.clients = new Array(params.parallel).fill(null);
    }

    static generate_id(params) {
        return JSON.stringify({
            protocol: 'ssh',
            host: params.host,
            user: params.username,
            password: params.password,
            port: params.port
        });
    }

    connect(slot) {
        return new Promise((resolve, reject) => {
            if (!this.connections[slot]) {
                const client = new Client();
                this.clients[slot] = client;
                client.on('ready', () => {
                    client.sftp((err, sftp) => {
                        if (err) {
                            reject(err);
                        } else {
                            this.logger.info(`SSH (slot ${slot}) connection established with ${this.params.host}`);
                            this.connections[slot] = sftp;
                            resolve(this.connections[slot]);
                        }
                    });
                });
                client.on('error', err => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject(`SSH (slot ${slot}) connection to host ${this.params.host} error: ${err}`);
                });
                client.on('end', () => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject(`SSH (slot ${slot}) connection to host ${this.params.host} disconnected`);
                });
                client.on('close', () => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject(`SSH (slot ${slot}) connection to host ${this.params.host} closed`);
                });
                client.connect({
                    host: this.params.host,
                    user: this.params.username,
                    password: this.params.password,
                    port: this.params.port,
                    keepaliveInterval: 10000
                });
            }
            return resolve(this.connections[slot]);
        });
    }

    disconnect(slot) {
        if (!this.connections[slot] || !this.clients[slot]) {
            return;
        }
        this.clients[slot].end();
        this.logger.info(`SSH (slot ${slot}) connection closed with ${this.params.host}`);
    }

    createReadStream(source, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug(`SSH (slot ${slot}) create read stream from: `, source);
            slot_control.keep_busy = true;

            const stream = connection.createReadStream(source, options);
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, control_release);
    }

    createWriteStream(target, options) {
        const control_release = true;
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug(`SSH (slot ${slot}) create write stream to: `, target);
            slot_control.keep_busy = true;

            const stream = connection.createWriteStream(target, options);
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, control_release);
    }

    mkdir(dir) {
        if (!dir || rootDirnames.includes(dir)) {
            return;
        }
        return this.stat(dir).catch(() => {}).then(stat => {
            if (stat) {
                throw stat.isDirectory() ? {exists: true} : `${dir} exists and is a file. Cannot create it as directory`;
            }
            return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
                this.logger.debug(`SSH (slot ${slot}) mkdir: `, dir);
                connection.mkdir(dir, err => (err ? reject(err.code === 2 ? {missing_parent: true} : err) : resolve()));
            }));
        }).catch(err => {
            if (!err || !err.exists) throw err;
            if (err && err.missing_parent) {
                return this.mkdir(path.dirname(dir)).then(() => this.mkdir(dir));
            }
        });
    }

    read(filename, params) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) download from: `, filename);
            if (params.start || params.end) {
                const stream = connection.createReadStream(filename);
                Base.get_data(stream, params.encoding).then(data => resolve(data)).catch(err => reject(err));
            } else {
                connection.readFile(filename, params, (err, contents) => (err ? reject(err) : resolve(contents)));
            }
        }));
    }

    stat(file) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) stat: `, file);
            connection.stat(file, (err, stat) => (err ? reject(err) : resolve(stat)));
        }));
    }

    write(target, contents = '', params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) upload to: `, target);
            if (params.start || params.end) {
                const stream = connection.createWriteStream(target, params);
                new Stream.Readable({
                    read() {
                        this.push(contents, params.encoding);
                        this.push(null);
                    }
                }).pipe(stream);
                stream.on('error', reject);
                stream.on('end', resolve);
                stream.on('close', resolve);
            } else {
                connection.writeFile(target, contents, params.encoding, err => (err ? reject(err) : resolve()));
            }
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
            this.logger.debug(`SSH (slot ${slot}) upload stream to: `, target);

            streams.writeStream = connection.createWriteStream(target);
            streams.writeStream.on('error', reject);
            streams.writeStream.on('close', resolve);

            streams.passThrough.on('error', reject);
            streams.passThrough.pipe(streams.writeStream);

            streams.readStream.on('error', reject);
            streams.readStream.pipe(streams.passThrough);

            publish(streams.readStream, size, params.publish);
        }));
    }

    link(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) link `, source, target);
            connection.ext_openssh_hardlink(source, target, err => (err ? reject(err) : resolve()));
        }));
    }

    symlink(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) symlink `, source, target);
            connection.symlink(source, target, err => (err ? reject(err) : resolve()));
        }));
    }

    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) remove: `, target);
            connection.unlink(target, err => (err ? reject(err) : resolve()));
        }));
    }

    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) move: `, source, target);
            connection.rename(source, target, err => (err ? reject(err) : resolve()));
        }));
    }

    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) list: `, dirname);
            connection.readdir(dirname, (err, list) => (err ? reject(err) : resolve(list)));
        })).then(list => {
            const results = [];
            for (let i = 0; i < list.length; i += 1) {
                list[i].attrs.name = list[i].filename;
                results.push(list[i].attrs);
            }
            return results;
        });
    }

    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SSH (slot ${slot}) list: `, dirname);
            connection.readdir(dirname, (err, list) => (err ? reject(err) : resolve(list)));
        })).then(list => list.reduce((p, file) => p.then(() => {
            const filename = path.join(dirname, file.filename);
            if (filename.match(ignored)) {
                return;
            }
            if (file.attrs.isDirectory()) {
                pending_paths.push(filename);
            } else {
                on_file(filename, file.attrs);
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

module.exports = SSH;
