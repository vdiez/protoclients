let moment = require('moment');
let chokidar = require('chokidar');
let fs = require('fs-extra');

module.exports = class {
    //(^|[\/\\])\.+([^\/\\\.]|$)/
    constructor({
            dirname,
            bucket,
            ignored = /(^|[\/\\])\../,
            on_watch_start = () => {},
            on_watch_stop = () => {},
            on_watch_complete = () => {},
            on_file_added = () => {},
            on_file_removed = () => {},
            on_error = () => {}
        },
        logger,
        connection
    ) {
        this.logger = logger;
        this.on_error = on_error;
        this.on_watch_complete = on_watch_complete;
        this.on_watch_start = on_watch_start;
        this.on_watch_stop = on_watch_stop;
        this.on_file_added = on_file_added;
        this.on_file_removed = on_file_removed;
        this.dirname = connection.constructor.normalize_path(dirname);
        this.bucket = bucket;
        this.ignored = ignored;
        this.fileObjects = {};
        this.timeout = null;
        this.connection = connection;
        this.started = false;
        this.update_settings({polling: connection.params.polling, polling_interval: connection.params.polling_interval});
    }

    update_settings({polling, polling_interval}) {
        if (this.connection.protocol === 'fs') {
            this.options = {};
            if (polling) this.options.usePolling = true;
            if (polling_interval) {
                this.options.interval = polling_interval;
                this.options.binaryInterval = polling_interval;
            }
        }
        else {
            if (polling && polling_interval) this.polling = polling_interval;
            else {
                clearTimeout(this.timeout);
                this.polling = false;
            }
        }
    }

    init_watcher() {
        if (this.connection.protocol === "fs") {
            return fs.mkdirp(this.dirname, {mode: 0o2775})
                .then(() => new Promise((resolve, reject) => {
                    this.watcher = chokidar.watch(this.dirname, {ignored: this.ignored, ignorePermissionErrors: true, ...this.options});
                    this.watcher.on('add', (path, stats) => this.on_file_added(this.connection.constructor.normalize_path(path), stats));
                    this.watcher.on('change', (path, stats) => this.on_file_added(this.connection.constructor.normalize_path(path), stats));
                    this.watcher.on('unlink', (path) => this.on_file_removed(this.connection.constructor.normalize_path(path)));
                    this.watcher.on('error', err => {
                        this.logger.error("Walk failed with dirname: ", this.dirname, err);
                        this.on_error(err);
                        reject();
                    });
                    this.watcher.on('ready', resolve);
                }))
        }

        this.now = moment().format('YYYYMMDDHHmmssSSS');
        return this.connection.walk({
            dirname: this.dirname,
            bucket: this.bucket,
            ignored: this.ignored,
            on_file: (filename, stats) => {
                if (!this.fileObjects[filename] || (this.fileObjects[filename] && stats.size !== this.fileObjects[filename].size)) {
                    this.logger.info(this.connection.protocol + " walk adding: ", filename);
                    this.on_file_added(filename, stats);
                }
                this.fileObjects[filename] = {last_seen: this.now, size: stats.size};
            },
            on_error: err => {
                this.logger.error(this.connection.protocol + " walk failed: ", err);
                this.on_error(err);
            }
        })
            .then(() => {
                for (let filename in this.fileObjects) {
                    if (this.fileObjects.hasOwnProperty(filename) && this.fileObjects[filename].last_seen !== this.now) {
                        this.on_file_removed(filename);
                        this.logger.info(this.protocol.toUpperCase() + " walk removing: ", filename);
                    }
                }
                if (this.polling) this.timeout = setTimeout(() => {
                    this.init_watcher();
                }, this.polling);
            })
            .catch(err => {
                this.logger.error("Walk failed with dirname: ", this.dirname, err);
                this.on_error(err);
            })
    }

    start_watch() {
        if (this.started) return;
        this.started = true;
        this.on_watch_start();
        return this.init_watcher().then(() => this.on_watch_complete());
    }

    stop_watch() {
        return Promise.resolve()
            .then(() => {
                this.started = false;
                if (this.connection.protocol === "fs") return this.watcher?.close();
                clearTimeout(this.timeout);
                this.polling = false;
                this.fileObjects = {};
                this.timeout = null;
            })
            .then(() => this.on_watch_stop());
    }
}