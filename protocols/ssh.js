let Client = require('ssh2').Client;
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = {
        host: {text: true},
        port: {number: true},
        username: {text: true},
        password: {secret: true},
        polling: {boolean: true},
        polling_interval: {number: true}
    };
    constructor(params, logger) {
        super(params, logger, "ssh");
        this.connection = null;
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'ssh', host: params.host, user: params.username, password: params.password, port: params.port});
    }
    update_settings(params) {
        params.parallel_parsers = 1;
        super.update_settings(params);
        if (this.id() !== this.constructor.generate_id(params)) this.disconnect();
    }
    connect(force) {
        if (this.connection && !force) return;
        return new Promise((resolve, reject) => {
            this.client = new Client();
            this.client
                .on('ready', () => {
                    this.client.sftp((err, sftp) => {
                        if (err) reject(err);
                        else {
                            this.logger.info("SSH connection established with " + this.params.host);
                            this.connection = sftp;
                            resolve();
                        }
                    });
                })
                .on('error', err => {
                    this.connection = null;
                    reject("SSH connection to host " + this.params.host + " error: " + err);
                })
                .on('end', () => {
                    this.connection = null;
                    reject("SSH connection to host " + this.params.host + " disconnected");
                })
                .on('close', () => {
                    this.connection = null;
                    reject("SSH connection to host " + this.params.host + " closed");
                })
                .connect({host: this.params.host, user: this.params.username, password: this.params.password, port: this.params.port, keepaliveInterval: 10000});
        });
    }
    disconnect() {
        return this.queue.run(() => this.client?.end()).then(() => this.logger.info("SSH connection closed with " + this.params.host));
    }
    createReadStream(source, options) {
        return this.wrapper(() => this.connection.createReadStream(source), options);
    }
    mkdir(dir) {
        if (!dir || dir === "/" || dir === ".") return;
        return this.stat(dir)
            .then(stat => {
                if (stat.isDirectory()) throw {exists: true};
                throw dir + " exists and is a file. Cannot create is as directory";
            })
            .then(() => this.wrapper(() => new Promise((resolve, reject) => {
                this.connection.mkdir(dir, err => {
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
    read(filename, encoding = 'utf8') {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.readFile(filename, {encoding: encoding}, (err, contents) => {
                if (err) reject(err);
                else resolve(contents);
            });
        }));
    }
    stat(file) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.stat(file, (err, stat) => {
                if (err) reject(err);
                else resolve(stat);
            });
        }));
    }
    write(target, contents = '', params = {}) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.writeFile(target, contents, params.encoding, err => {
                if (err) reject(err);
                else resolve();
            })
        }));
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: "local copy not implemented for " + this.protocol, not_implemented: 1}
        return this.wrapper(() => new Promise((resolve, reject) => {
            streams.writeStream = this.connection.createWriteStream(target);
            streams.writeStream.on('error', err => reject(err));
            streams.readStream.on('error', err => reject(err));
            streams.passThrough.on('error', err => reject(err));
            streams.writeStream.on('close', () => resolve());
            streams.readStream.pipe(streams.passThrough);
            streams.passThrough.pipe(streams.writeStream);
            publish(streams.readStream, size, params.publish);
        }));
    }
    link(source, target) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.ext_openssh_hardlink(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    symlink(source, target) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.symlink(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    remove(target) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.unlink(target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    move(source, target) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.rename(source, target, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    walk(dirname, ignored) {
        return this.wrapper(() =>  new Promise((resolve, reject) => {
            this.connection.readdir(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }))
        .then(list => list.reduce((p, file) => p
            .then(() => {
                let filename = path.posix.join(dirname, file.filename);
                if (filename.match(ignored)) return;
                if (file.attrs.isDirectory()) return this.walk(filename, ignored);
                if (!this.fileObjects[filename] || (this.fileObjects[filename] && file.attrs.size !== this.fileObjects[filename].size)) this.on_file_added(filename, file.attrs);
                this.fileObjects[filename] = {last_seen: this.now, size: file.attrs.size};
            })
            .catch(err => {
                this.logger.error("SSH walk for '" + file + "' failed: ", err);
                this.on_error(err);
            }), Promise.resolve()))
    }
}
