let path = require('path');
const get_stream = require('get-stream');
const is_stream = require('is-stream');
let moment = require('moment');

module.exports = class {
    static parameters = ["parallel_parsers", "polling", "polling_interval"];
    constructor(params, logger, protocol) {
        this.logger = logger;
        this.protocol = protocol;
        this.params = {};
        this.fileObjects = {};
        this.timeout = null;
        this.disconnect_timeout = null;
        this.on_error = () => {};
        this.on_watch_complete = () => {};
        this.on_watch_start = () => {};
        this.on_watch_stop = () => {};
        this.on_file_added = () => {};
        this.on_file_removed = () => {};
        this.update_settings(params);
    }
    id() {
        return this.constructor.generate_id(this.params);
    }
    update_settings(params) {
        this.params = params;
        if (params.polling && params.polling_interval) this.polling = params.polling_interval;
        else {
            clearTimeout(this.timeout);
            this.polling = false;
        }
        this.queue = require("parallel_limit")(params.parallel_parsers);
    }
    wrapper(f) {
        return this.queue.run(() => Promise.resolve()
            .then(() => {
                clearTimeout(this.disconnect_timeout);
                return this.connect();
            })
            .then(() => f())
            .then(result => {
                this.disconnect_timeout = setTimeout(() => {
                    this.disconnect();
                }, 300000)
                return result;
            })
        );
    }
    walk(dirname, ignored) {}
    init_watcher(dirname, ignored) {
        if (!this.started) return;
        this.now = moment().format('YYYYMMDDHHmmssSSS');
        return this.walk(dirname, ignored)
            .then(() => {
                for (let filename in this.fileObjects) {
                    if (this.fileObjects.hasOwnProperty(filename) && this.fileObjects[filename].last_seen !== this.now) this.on_file_removed(path);
                }
                if (this.polling) this.timeout = setTimeout(() => {
                    this.init_watcher(dirname, ignored);
                }, this.polling);
            })
            .catch(err => {
                this.logger.error("Walk failed with dirname: ", dirname, err);
                this.on_error(err);
            })
    }
    start_watch(dirname, ignored = /(^|[\/\\])\../) {//(^|[\/\\])\.+([^\/\\\.]|$)/
        if (this.started) return;
        this.started = true;
        this.on_watch_start();
        return this.init_watcher(this.constructor.normalize_path(dirname), ignored).then(() => this.on_watch_complete());
    }
    stop_watch(kill) {
        clearTimeout(this.timeout);
        this.polling = false;
        this.started = false;
        this.on_watch_stop();
        return Promise.resolve()
            .then(() => this.destroy_watcher())
            .then(() => {
                this.fileObjects = {};
                this.timeout = null;
                if (kill) return this.disconnect();
            });
    }
    destroy_watcher() {}
    connect() {}
    disconnect() {}
    createReadStream(source) {throw {message: "createReadStream method not implemented for " + this.protocol, not_implemented: 1}}
    mkdir(dir) {throw {message: "mkdir method not implemented for " + this.protocol, not_implemented: 1}}
    stat(target) {throw {message: "stat method not implemented for " + this.protocol, not_implemented: 1}}
    read(source) {throw {message: "read method not implemented for " + this.protocol, not_implemented: 1}}
    write(target, contents = '') {throw {message: "write method not implemented for " + this.protocol, not_implemented: 1}}
    copy(source, target, streams, size, params) {throw {message: "copy method not implemented for " + this.protocol, not_implemented: 1}}
    link(source, target) {throw {message: "link method not implemented for " + this.protocol, not_implemented: 1}}
    symlink(source, target) {throw {message: "symlink method not implemented for " + this.protocol, not_implemented: 1}}
    move(source, target) {throw {message: "move method not implemented for " + this.protocol, not_implemented: 1}}
    remove(target) {throw {message: "remove method not implemented for " + this.protocol, not_implemented: 1}}
    tag(target) {throw {message: "tag method not implemented for " + this.protocol, not_implemented: 1}}
    static filename(dirname, uri) {
        return uri.slice(dirname.length + 1);
    }
    static path(dirname, filename) {
        return path.posix.join(this.normalize_path(dirname), filename);
    }
    static normalize_path(dirname) {
        return path.posix.normalize(dirname.replace(/[\\\/]+/g, "/")).replace(/^(.+?)\/*?$/, "$1");  //remove trailing slashes unless it's root path
    }
    static get_data(data, encoding = 'utf-8') {
        if (typeof data === "string") return data;
        return Promise.resolve()
            .then(() => {
                if (is_stream.readable(data)) return get_stream.buffer(data);
                return data;
            })
            .then(() => {
                if (encoding && Buffer.isBuffer(data)) return data.toString(encoding);
                return data;
            })
    }
}