const fs = require('fs-extra');
// Can we remove posix ?
const path = require('path').posix;
const Stream = require('stream');
const Base = require('./Base');
const publish = require('../default_publish');

class FS extends Base {
    constructor(params, logger) {
        super('fs', params, logger);
    }

    static generate_id() {
        return 'fs';
    }

    createReadStream(source, options) {
        const control_release = true;
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`FS (slot ${slot}) create read stream from: `, source);
            slot_control.keep_busy = true;

            const stream = fs.createReadStream(source, options);
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, control_release);
    }

    createWriteStream(target, options) {
        const control_release = true;
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`FS (slot ${slot}) create write stream to: `, target);
            slot_control.keep_busy = true;

            const stream = fs.createWriteStream(target, options);
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, control_release);
    }

    mkdir(dir) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) mkdir: `, dir);
            return fs.ensureDir(dir);
        });
    }

    write(target, contents = '', params) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) write: `, target);
            if (params.start || params.end) {
                return new Promise((resolve, reject) => {
                    const stream = fs.createWriteStream(target, params);
                    new Stream.Readable({
                        read() {
                            this.push(contents, params.encoding);
                            this.push(null); // https://nodejs.org/api/stream.html#stream_readable_push_chunk_encoding
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

    read(filename, params) {
        return this.queue.run(slot => {
            this.logger.debug(`FS (slot ${slot}) read: `, filename);
            if (params.start || params.end) {
                const stream = fs.createReadStream(filename, params);
                return Base.get_data(stream, params.encoding);
            }
            return fs.readFile(filename, params);
        });
    }

    copy(source, target, streams, size, params) {
        return this.queue.run(slot => new Promise((resolve, reject) => {
            this.logger.debug(`FS (slot ${slot}) copy stream to: `, target);

            streams.writeStream = fs.createWriteStream(target);
            streams.writeStream.on('error', err => reject(err));
            streams.writeStream.on('close', () => resolve());

            streams.passThrough.on('error', err => reject(err));
            streams.passThrough.pipe(streams.writeStream);

            if (!streams.readStream) {
                streams.readStream = fs.createReadStream(source);
            }
            streams.readStream.on('error', err => reject(err));
            streams.readStream.pipe(streams.passThrough);

            publish(streams.readStream, size, params.publish);
        }));
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
            return fs.readdir(dirname).then(files => {
                const stats = files.map(file => fs.stat(path.join(dirname, file)).then(stat => {
                    stat.name = file;
                    return stat;
                }));
                return Promise.all(stats);
            });
        });
    }

    static normalize_path(uri) {
        uri = Base.normalize_path(uri);
        if (process.platform === 'win32') {
            if (uri.startsWith('/')) {
                uri = `C:${uri}`;
            }
            uri = uri[0].toUpperCase() + uri.slice(1);
        }
        return uri;
    }
}

module.exports = FS;
