let path = require('path');
const get_stream = require('get-stream');
const is_stream = require('is-stream');
let watcher = require('./watcher');

module.exports = class {
    static parameters = {
        parallel: {type: "number"},
        polling: {type: "boolean"},
        polling_interval: {type: "number"}
    };
    static accept_ranges = true;
    constructor(params, logger, protocol) {
        this.logger = logger;
        this.protocol = protocol;
        this.params = {};
        this.disconnect_timeout = [];
        this.queue = require("parallel_limit")(params.parallel);
        this.connections = new Array(params.parallel).fill(null);
        this.update_settings(params);
    }
    id() {
        return this.constructor.generate_id(this.params);
    }
    update_settings(params) {
        this.params = params;
        this.queue.set_size(params.parallel);
    }
    wrapper(f, control_release) {
        return this.queue.run((slot, slot_control) => Promise.resolve()
            .then(() => {
                clearTimeout(this.disconnect_timeout[slot]);
                return this.connect(slot);
            })
            .then(connection => f(connection, slot, slot_control))
            .then(result => {
                Promise.resolve()
                    .then(() => {
                        if (control_release && slot_control?.keep_busy) return slot_control.release_promise;
                    })
                    .then(() => {
                        this.disconnect_timeout[slot] = setTimeout(() => {
                            this.disconnect(slot);
                        }, 300000)
                    })
                return result;
            }), control_release);
    }
    create_watcher(params) {
        return new watcher(params, this.logger, this);
    }
    connect() {}
    disconnect() {}
    createReadStream(source) {throw {message: "createReadStream method not implemented for " + this.protocol, not_implemented: 1}}
    createWriteStream(target) {throw {message: "createWriteStream method not implemented for " + this.protocol, not_implemented: 1}}
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
    list(dirname) {throw {message: "list method not implemented for " + this.protocol, not_implemented: 1}}
    walk(params) {throw {message: "walk method not implemented for " + this.protocol, not_implemented: 1}}
    static get_filename(dirname, uri) {
        if (dirname === "." || dirname === "/" || dirname === "./" || dirname === "") return uri.replace(/^[\/]+/, "");
        return uri.slice(dirname.length).replace(/^\/+/, "");
    }
    static normalize_path(dirname) {
        return path.posix.normalize((dirname || "").replace(/[\\\/]+/g, "/")).replace(/^(.+?)\/*?$/, "$1");  //remove trailing slashes unless it's root path
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