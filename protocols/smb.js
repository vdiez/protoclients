const Client = require('@marsaud/smb2');
const Stream = require('stream');
const path = require('path');
const base = require('../base');

module.exports = class extends base {
    static parameters = {
        ...base.parameters,
        parallel: {type: 'number', max: 50},
        host: {type: 'text'},
        share: {type: 'text'},
        domain: {type: 'text'},
        port: {type: 'number', min: 0, max: 65535},
        username: {type: 'text'},
        password: {type: 'secret'}
    };

    constructor(params, logger) {
        super(params, logger, 'smb');
    }

    static generate_id(params) {
        return JSON.stringify({protocol: 'smb', share: `\\\\${params.host}\\${params.share}`, username: params.username, password: params.password, port: params.port, domain: params.domain});
    }

    connect(slot) {
        if (!this.connections[slot]) {
            this.connections[slot] = new Client({share: `\\\\${this.params.host}\\${this.params.share}`, username: this.params.username, password: this.params.password, port: this.params.port, domain: this.params.domain, autoCloseTimeout: 0});
            this.logger.info(`SMB (slot ${slot}) connection established with ${this.params.host}`);
        }
        return this.connections[slot];
    }

    disconnect(slot) {
        if (!this.connections[slot]) return;
        this.connections[slot].disconnect();
        this.logger.info(`SMB (slot ${slot}) connection closed with ${this.params.host}`);
    }

    createReadStream(source, params) {
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) create read stream from: `, source);
            connection.createReadStream(source.replace(/\//g, '\\'), params, (err, stream) => {
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

    createWriteStream(target, params) {
        return this.wrapper((connection, slot, slot_control) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) create write stream to: `, target);
            connection.createWriteStream(target.replace(/\//g, '\\'), params, (err, stream) => {
                if (err) reject(err);
                else {
                    slot_control.keep_busy = true;
                    stream.on('error', slot_control.release_slot);
                    stream.on('finish', slot_control.release_slot);
                    stream.on('close', slot_control.release_slot);
                    resolve(stream);
                }
            });
        }), true);
    }

    mkdir(dir) {
        if (!dir || dir === '/' || dir === '.') return;
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) mkdir: `, dir);
            connection.mkdir(dir.replace(/\//g, '\\'), err => {
                if (err && err.code !== 'STATUS_OBJECT_NAME_COLLISION') reject(err);
                else resolve();
            });
        }));
    }

    read(filename, params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) download from: `, filename);
            if (params.start || params.end) {
                connection.createReadStream(filename.replace(/\//g, '\\'), params, (err, stream) => {
                    if (err) reject(err);
                    else this.constructor.get_data(stream, params.encoding).then(data => resolve(data)).catch(err => reject(err));
                });
            }
            else {
                connection.readFile(filename.replace(/\//g, '\\'), {encoding: params.encoding}, (err, contents) => {
                    if (err) reject(err);
                    else resolve(contents);
                });
            }
        }));
    }

    write(target, contents = Buffer.alloc(0), params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) upload to: `, target);
            if (params.start || params.end) {
                connection.createWriteStream(target.replace(/\//g, '\\'), params, (err, stream) => {
                    if (err) reject(err);
                    else {
                        new Stream.Readable({
                            read() {
                                this.push(contents, params.encoding);
                                this.push(null);
                            }
                        }).pipe(stream);
                        stream.on('error', reject);
                        stream.on('end', resolve);
                        stream.on('close', resolve);
                    }
                });
            }
            else {
                connection.writeFile(target, contents, {encoding: params.encoding}, err => {
                    if (err) reject(err);
                    else resolve();
                });
            }
        }));
    }

    stat(filename) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) stat: `, filename);
            connection.stat(filename.replace(/\//g, '\\'), (err, stat) => {
                if (err) reject(err);
                else resolve(stat);
            });
        }));
    }

    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) remove: `, target);
            connection.unlink(target.replace(/\//g, '\\'), err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }

    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) move: `, source, target);
            connection.rename(source.replace(/\//g, '\\'), target.replace(/\//g, '\\'), {replace: true}, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }

    list(dirname) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug(`SMB (slot ${slot}) list: `, dirname);
            connection.readdir(dirname.replace(/\//g, '\\'), {stats: true}, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            });
        }));
    }

    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.list(dirname)
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    const filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.isDirectory()) pending_paths.push(filename);
                    else on_file(filename, {size: file.size, mtime: file.mtime, isDirectory: () => false});
                })
                .catch(on_error), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk({dirname: pending_paths.shift(), ignored, on_file, on_error, pending_paths});
            });
    }
};
