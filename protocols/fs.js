let base = require("../base");
let chokidar = require('chokidar');
let fs = require('fs-extra');
let publish = require('../default_publish');

module.exports = class extends base {
    constructor(params, logger) {
        super(params, logger, "fs");
    }
    static generate_id(params) {
        return 'fs';
    }
    update_settings(params) {
        super.update_settings(params);
        this.options = {};
        if (params.polling) this.options.usePolling = true;
        if (params.polling_interval) {
            this.options.interval = params.polling_interval;
            this.options.binaryInterval = params.polling_interval;
        }
    }
    init_watcher(dirname, ignored) {
        return fs.mkdirp(dirname, {mode: 0o2775})
            .then(() => new Promise((resolve, reject) => {
                this.watcher = chokidar.watch(dirname, {ignored: ignored, ignorePermissionErrors: true, ...this.options});
                this.watcher.on('add', (path, stats) => this.on_file_added(this.constructor.normalize_path(path), stats));
                this.watcher.on('change', (path, stats) => this.on_file_added(this.constructor.normalize_path(path), stats));
                this.watcher.on('unlink', (path) => this.on_file_removed(path));
                this.watcher.on('error', err => {
                    this.logger.error("Walk failed with dirname: ", dirname, err);
                    this.on_error(err);
                    reject();
                });
                this.watcher.on('ready', resolve);
            }))
    }
    stop_watch() {
        return this.queue.run(slot => {
            this.logger.info("FS (slot " + slot + ") closing watcher");
            return this.watcher?.close();
        }).then(() => super.stop_watch());
    }
    createReadStream(source, options) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug("FS (slot " + slot + ") create stream from: ", source);
            let stream = fs.createReadStream(source, options);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
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
    static normalize_path(uri) {
        uri = super.normalize_path(uri);
        if (process.platform === "win32") {
            if (uri.startsWith('/')) uri = "C:" + uri;
            uri = uri[0].toUpperCase() + uri.slice(1);
        }
        return uri;
    }
}
