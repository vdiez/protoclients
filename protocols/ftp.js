let Client = require('ftp');
let path = require('path');
let base = require("../base");
let publish = require('../default_publish');

module.exports = class extends base {
    static parameters = ["polling", "polling_interval", "host", "port", "username", "password", "secure"];
    constructor(params, logger) {
        super(params, logger, "ftp");
        this.connection = null;
    }
    static generate_id(params) {
        return JSON.stringify({protocol: 'ftp', host: params.host, user: params.username, password: params.password, port: params.port, secure: params.secure});
    }
    update_settings(params) {
        params.parallel_parsers = 1;
        if (this.id() !== this.constructor.generate_id(params)) this.disconnect();
        super.update_settings();
    }
    connect(force) {
        if (this.connection && !force) return;
        return new Promise((resolve, reject) => {
            this.connection = new Client();
            this.connection
                .on('ready', () => {
                    this.logger.info("FTP connection established with " + this.params.host);
                    resolve();
                })
                .on('error', err => {
                    this.connection = null;
                    reject("FTP connection to host " + this.params.host + " error: " + err);
                })
                .on('end', () => {
                    this.connection = null;
                    reject("FTP connection ended to host " + this.params.host);
                })
                .on('close', had_error => {
                    this.connection = null;
                    reject("FTP connection lost to host " + this.params.host + ". Due to error: " + had_error);
                })
                .connect({host: this.params.host, user: this.params.username, password: this.params.password, port: this.params.port, secure: this.params.secure, secureOptions: {rejectUnauthorized: false}});
        });
    }
    disconnect() {
        return this.queue.run(() => this.connection.end());
    }
    createReadStream(source) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.get(source, (err, stream) => {
                if (err) reject(err);
                else resolve(stream);
            });
        }));
    }
    mkdir(dir) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            if (dir === "." || dir === "/" || dir === "") resolve();
            this.connection.mkdir(dir, true, err => {
                if (err) reject(err);
                else resolve();
            });
        }));
    }
    list(dirname) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.list(dirname, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            })
        }));
    }
    read(filename, encoding = 'utf-8') {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.get(filename, (err, stream) => {
                if (err) reject(err);
                else this.constructor.get_data(stream, encoding).then(data => resolve(data)).catch(err => reject(err));
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
    write(target, contents = '') {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.put(contents, target, err => {
                if (err) reject(err);
                else resolve();
            })
        }));
    }
    copy(source, target, streams, size, params) {
        if (!streams.readStream) throw {message: "local copy not implemented for " + this.protocol, not_implemented: 1}
        return this.wrapper(() => new Promise((resolve, reject) => {
            streams.readStream.on('error', err => reject(err));
            streams.passThrough.on('error', err => reject(err));
            streams.readStream.pipe(streams.passThrough);

            this.connection.put(streams.passThrough, target, err => {
                if (err) reject(err);
                else resolve();
            });
            publish(streams.readStream, size, params.publish);
        }));
    }
    remove(target) {
        return this.wrapper(() => new Promise((resolve, reject) => {
            this.connection.delete(target, err => {
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
        return this.list(dirname)
            .then(list => list.reduce((p, file) => p
                .then(() => {
                    let filename = path.posix.join(dirname, file.name);
                    if (filename.match(ignored)) return;
                    if (file.type === 'd') return this.walk(filename, ignored);
                    if (!this.fileObjects[filename] || (this.fileObjects[filename] && file.size !== this.fileObjects[filename].size)) this.on_file_added(filename, {size: file.size, mtime: file.date, isDirectory: () => false})
                    this.fileObjects[filename] = {last_seen: this.now, size: file.size};
                })
                .catch(err => {
                    this.logger.error("FTP walk for '" + file + "' failed: ", err);
                    this.on_error(err);
                }), Promise.resolve()));
    }
}
