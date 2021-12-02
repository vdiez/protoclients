const fs = require('fs-extra');
const Stream = require('stream');
const path = require('path');
const base = require('../base');

module.exports = class extends base {
    constructor(params, logger) {
        super(params, logger, 'fs');
    }

    static generate_id() {
        return 'fs';
    }

    createReadStream(source, params) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`FS (slot ${slot}) create read stream from: `, source);
            const stream = fs.createReadStream(source, params);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }

    createWriteStream(target, params) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`FS (slot ${slot}) create write stream to: `, target);
            const stream = fs.createWriteStream(target, params);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }

    mkdir(dir) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) mkdir: `, dir);
            return fs.ensureDir(dir);
        });
    }

    write(target, contents = '', params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) write: `, target);
            if (params.start || params.end) {
                return new Promise((resolve, reject) => {
                    const stream = fs.createWriteStream(target, params);
                    new Stream.Readable({
                        read() {
                            this.push(contents, params.encoding);
                            this.push(null);
                        }
                    }).pipe(stream);
                    stream.on('error', reject);
                    stream.on('end', resolve);
                    stream.on('close', resolve);
                });
            }
            return fs.writeFile(target, contents);
        });
    }

    read(filename, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) read: `, filename);
            if (params.start || params.end) {
                const stream = fs.createReadStream(filename, params);
                return this.constructor.get_data(stream, params.encoding);
            }
            return fs.readFile(filename, params);
        });
    }

    link(source, target) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) link: `, source, target);
            return fs.ensureLink(source, target);
        });
    }

    symlink(source, target) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) symlink: `, source, target);
            return fs.ensureSymlink(source, target);
        });
    }

    remove(target) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) remove: `, target);
            return fs.remove(target);
        });
    }

    move(source, target) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) move: `, source, target);
            return fs.rename(source, target);
        });
    }

    stat(filename) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) stat: `, filename);
            return fs.stat(filename);
        });
    }

    list(dirname) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) readdir: `, dirname);
            return fs.readdir(dirname)
                .then(files => {
                    const stats = [];
                    return files.reduce((p, file) => p.then(() => fs.stat(path.posix.join(dirname, file)).then(stat => {stat.name = file; stats.push(stat);}).catch(() => {})), Promise.resolve())
                        .then(() => stats);
                });
        });
    }

    static normalize_path(uri) {
        uri = super.normalize_path(uri);
        if (process.platform === 'win32') {
            if (uri.startsWith('/')) uri = `C:${uri}`;
            uri = uri[0].toUpperCase() + uri.slice(1);
        }
        return uri;
    }
};
