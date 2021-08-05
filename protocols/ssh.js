let Client = require('ssh2').Client;
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = {
        parallel: {type: "number"},
        host: {type: "text"},
        port: {type: "number"},
        username: {type: "text"},
        password: {type: "secret"},
        polling: {type: "boolean"},
        polling_interval: {type: "number"}
    };
    constructor(params, logger) {
        super(params, logger, "ssh");
        this.clients = new Array(params.parallel).fill(null);
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'ssh', host: params.host, user: params.username, password: params.password, port: params.port});
    }
    connect(slot) {
        if (this.connections[slot]) return this.connections[slot];
        return new Promise((resolve, reject) => {
            this.clients[slot] = new Client();
            this.clients[slot]
                .on('ready', () => {
                    this.clients[slot].sftp((err, sftp) => {
                        if (err) reject(err);
                        else {
                            this.logger.info("SSH (slot " + slot + ") connection established with " + this.params.host);
                            this.connections[slot] = sftp;
                            resolve(this.connections[slot]);
                        }
                    });
                })
                .on('error', err => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject("SSH (slot " + slot + ") connection to host " + this.params.host + " error: " + err);
                })
                .on('end', () => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject("SSH (slot " + slot + ") connection to host " + this.params.host + " disconnected");
                })
                .on('close', () => {
                    this.connections[slot] = null;
                    this.clients[slot] = null;
                    reject("SSH (slot " + slot + ") connection to host " + this.params.host + " closed");
                })
                .connect({host: this.params.host, user: this.params.username, password: this.params.password, port: this.params.port, keepaliveInterval: 10000});
        });
    }
    disconnect(slot) {
        if (!this.connections[slot]) return;
        this.clients[slot].end();
        this.logger.info("SSH (slot " + slot + ") connection closed with " + this.params.host);
    }
    createReadStream(source, options) {
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug("SSH (slot " + slot + ") create read stream from: ", source);
            let stream = connection.createReadStream(source, options)
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }
    createWriteStream(target, options) {
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug("SSH (slot " + slot + ") create write stream to: ", target);
            let stream = connection.createWriteStream(target, options)
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }
    mkdir(dir) {
        if (!dir || dir === "/" || dir === ".") return;
        return this.stat(dir).catch(() => {})
            .then(stat => {
                if (stat) {
                    if (stat.isDirectory()) throw {exists: true};
                    throw dir + " exists and is a file. Cannot create it as directory";
                }
            })
            .then(() => this.wrapper((connection, slot) => new Promise((resolve, reject) => {
                this.logger.debug("SSH (slot " + slot + ") mkdir: ", dir);
                connection.mkdir(dir, err => {
                    if (!err) resolve();
                    else if (err && err.code === 2) reject({missing_parent: true});
                    else reject(err);
                })
            })))
            .catch(err => {
                if (err && err.missing_parent) return this.mkdir(path.posix.dirname(dir)).then(() => this.mkdir(dir));
                else if (!err || !err.exists) throw err;
            });
    }
    read(filename, params) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") download from: ", filename);
            if (params.start || params.end) {
                let stream = connection.createReadStream(filename)
                this.constructor.get_data(stream, params.encoding).then(data => resolve(data)).catch(err => reject(err));
            }
            else {
                connection.readFile(filename, params, (err, contents) => {
                    if (err) reject(err);
                    else resolve(contents);
                });
            }
        }));
    }
    stat(file) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") stat: ", file);
            connection.stat(file, (err, stat) => {
                if (err) reject(err);
                else resolve(stat);
            });
        }));
    }
    write(target, contents = '', params = {}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") upload to: ", target);
            if (params.start || params.end) {
                let stream = connection.createWriteStream(target, params);
                new (require('stream').Readable)({read() {this.push(contents, params.encoding);this.push(null);}}).pipe(stream);
                stream.on('error', reject);
                stream.on('end', resolve);
                stream.on('close', resolve);
            }
            else {
                connection.writeFile(target, contents, params.encoding, err => {
                    if (err) reject(err);
                    else resolve();
                })
            }
        }));
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: "local copy not implemented for " + this.protocol, not_implemented: 1}
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") upload stream to: ", target);
            streams.writeStream = connection.createWriteStream(target);
            streams.writeStream.on('error', reject);
            streams.readStream.on('error', reject);
            streams.passThrough.on('error', reject);
            streams.writeStream.on('close', resolve);
            streams.readStream.pipe(streams.passThrough);
            streams.passThrough.pipe(streams.writeStream);
            publish(streams.readStream, size, params.publish);
        }));
    }
    link(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") link ", source, target);
            connection.ext_openssh_hardlink(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    symlink(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") symlink ", source, target);
            connection.symlink(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    remove(target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") remove: ", target);
            connection.unlink(target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    move(source, target) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") move: ", source, target);
            connection.rename(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    list(dirname, ignored) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") list: ", dirname);
            connection.readdir(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }))
        .then(list => {
            let results = [];
            for (let i = 0; i < list.length; i++) {
                list[i].attrs.name = list[i].filename;
                results.push(list[i].attrs);
            }
            return results;
        })
    }
    walk({dirname, ignored, on_file, on_error, pending_paths = []}) {
        return this.wrapper((connection, slot) => new Promise((resolve, reject) => {
            this.logger.debug("SSH (slot " + slot + ") list: ", dirname);
            connection.readdir(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }))
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    let filename = path.posix.join(dirname, file.filename);
                    if (filename.match(ignored)) return;
                    if (file.attrs.isDirectory()) pending_paths.push(filename);
                    else on_file(filename, file.attrs);
                })
                .catch(on_error), Promise.resolve()))
            .then(() => {
                if (pending_paths.length) return this.walk({dirname: pending_paths.shift(), ignored, on_file, on_error, pending_paths});
            })
    }
}
