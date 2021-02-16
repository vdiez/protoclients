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
    disconnect() {
        return this.queue.run(() => this.watcher?.close());
    }
    createReadStream(source, options) {
        return this.queue.run(() => fs.createReadStream(source, options));
    }
    mkdir(dir) {
        return this.queue.run(() => fs.ensureDir(dir));
    }
    write(target, contents = '') {
        return this.queue.run(() => fs.writeFile(target, contents));
    }
    read(filename, encoding = 'utf8') {
        return this.queue.run(() => fs.readFile(filename, encoding));
    }
    copy(source, target, streams, size, params) {
        return this.queue.run(() => new Promise((resolve, reject) => {
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
        return this.queue.run(() => fs.ensureLink(source, target));
    }
    symlink(source, target) {
        return this.queue.run(() => fs.ensureSymlink(source, target));
    }
    remove(target) {
        return this.queue.run(() => fs.remove(target))
    }
    move(source, target) {
        return this.queue.run(() => fs.rename(source, target))
    }
    stat(filename) {
        return this.queue.run(() => fs.stat(filename));
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
