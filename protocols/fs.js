let base = require("../base");
let fs = require('fs-extra');
let publish = require('../default_publish');
let path = require("path");

module.exports = class extends base {
    constructor(params, logger) {
        super(params, logger, "fs");
    }
    static generate_id(params) {
        return 'fs';
    }
    createReadStream(source, options) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug("FS (slot " + slot + ") create read stream from: ", source);
            let stream = fs.createReadStream(source, options);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }
    createWriteStream(target, options) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug("FS (slot " + slot + ") create write stream to: ", target);
            let stream = fs.createWriteStream(target, options);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }
    mkdir(dir) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") mkdir: ", dir);
            return fs.ensureDir(dir);
        });
    }
    write(target, contents = '') {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") write: ", target);
            return fs.writeFile(target, contents);
        });
    }
    read(filename, encoding = 'utf8') {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") read: ", filename);
            return fs.readFile(filename, encoding);
        });
    }
    copy(source, target, streams, size, params) {
        return this.queue.run(slot => new Promise((resolve, reject) => {
            this.logger.debug("FS (slot " + slot + ") copy stream to: ", target);
            if (!streams.readStream) streams.readStream = fs.createReadStream(source)
            streams.writeStream = fs.createWriteStream(target);
            streams.writeStream.on('error', err => reject(err));
            streams.readStream.on('error', err => reject(err));
            streams.passThrough.on('error', err => reject(err));
            streams.writeStream.on('close', () => resolve());
            streams.readStream.pipe(streams.passThrough);
            streams.passThrough.pipe(streams.writeStream);
            publish(streams.readStream, size, params.publish);
        }))
    }
    link(source, target) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") link: ", source, target);
            return fs.ensureLink(source, target)
        });
    }
    symlink(source, target) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") symlink: ", source, target);
            return fs.ensureSymlink(source, target)
        });
    }
    remove(target) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") remove: ", target);
            return fs.remove(target)
        })
    }
    move(source, target) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") move: ", source, target);
            return fs.rename(source, target);
        })
    }
    stat(filename) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") stat: ", filename);
            return fs.stat(filename);
        });
    }
    list(dirname) {
        return this.queue.run(slot => {
            this.logger.debug("FS (slot " + slot + ") readdir: ", dirname);
            return fs.readdir(dirname)
            .then(files => {
                let stats = [];
                return files.reduce((p, file) => p.then(() => fs.stat(path.posix.join(dirname, file)).then(stat => {stat.name = file; stats.push(stat)}).catch(() => {})), Promise.resolve())
                    .then(() => stats)
            })
        });
    }
    static normalize_path(uri) {
        uri = super.normalize_path(uri);
        if (process.platform === "win32") {
            if (uri.startsWith('/')) uri = "C:" + uri;
            uri = uri[0].toUpperCase() + uri.slice(1);
        }
        return uri;
    }
}
