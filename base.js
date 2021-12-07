const path = require('path');
const get_stream = require('get-stream');
const is_stream = require('is-stream');
const queue = require('parallel_limit');
const Watcher = require('./watcher');

module.exports = class {
    static parameters = {
        parallel: {type: 'number', max: 500},
        polling: {type: 'boolean'},
        polling_interval: {type: 'number', min: 5000}
    };

    static accept_ranges = true;

    constructor(params, logger, protocol) {
        this.logger = logger;
        this.protocol = protocol;
        this.params = {};
        this.disconnect_timeout = [];
        this.queue = queue(params.parallel);
        this.connections = new Array(params.parallel).fill(null);
        this.update_settings(params);
    }

    id() {
        return this.constructor.generate_id(this.params);
    }

    update_settings(params = {}) {
        this.params = params;
        this.queue.size = params.parallel || 1;
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
                        }, 300000);
                    });
                return result;
            })
            .catch(err => {
                if (control_release && slot_control?.keep_busy) slot_control.release_slot();
                this.disconnect_timeout[slot] = setTimeout(() => {
                    this.disconnect(slot);
                }, 300000);
                throw err;
            }), control_release);
    }

    create_watcher(params) {
        return new Watcher(params, this.logger, this);
    }

    // eslint-disable-next-line class-methods-use-this
    connect() {}

    // eslint-disable-next-line class-methods-use-this
    disconnect() {}

    createReadStream() {throw {message: `createReadStream method not implemented for ${this.protocol}`, not_implemented: 1};}

    createWriteStream() {throw {message: `createWriteStream method not implemented for ${this.protocol}`, not_implemented: 1};}

    mkdir() {throw {message: `mkdir method not implemented for ${this.protocol}`, not_implemented: 1};}

    stat() {throw {message: `stat method not implemented for ${this.protocol}`, not_implemented: 1};}

    read() {throw {message: `read method not implemented for ${this.protocol}`, not_implemented: 1};}

    write() {throw {message: `write method not implemented for ${this.protocol}`, not_implemented: 1};}

    copy() {throw {message: `copy method not implemented for ${this.protocol}`, not_implemented: 1};}

    link() {throw {message: `link method not implemented for ${this.protocol}`, not_implemented: 1};}

    symlink() {throw {message: `symlink method not implemented for ${this.protocol}`, not_implemented: 1};}

    move() {throw {message: `move method not implemented for ${this.protocol}`, not_implemented: 1};}

    remove() {throw {message: `remove method not implemented for ${this.protocol}`, not_implemented: 1};}

    tag() {throw {message: `tag method not implemented for ${this.protocol}`, not_implemented: 1};}

    list() {throw {message: `list method not implemented for ${this.protocol}`, not_implemented: 1};}

    walk() {throw {message: `walk method not implemented for ${this.protocol}`, not_implemented: 1};}

    static get_filename(dirname, uri) {
        if (dirname === '.' || dirname === '/' || dirname === './' || dirname === '') return uri.replace(/^[/]+/, '');
        return uri.slice(dirname.length).replace(/^\/+/, '');
    }

    static normalize_path(dirname) {
        return path.posix.normalize((dirname || '').replace(/[\\/]+/g, '/')).replace(/^(.+?)\/*?$/, '$1'); //remove trailing slashes unless it's root path
    }

    static get_data(data, encoding = 'utf-8') {
        if (typeof data === 'string') return data;
        return Promise.resolve()
            .then(() => {
                if (is_stream.readable(data)) return get_stream.buffer(data);
                return data;
            })
            .then(() => {
                if (encoding && Buffer.isBuffer(data)) return data.toString(encoding);
                return data;
            });
    }
};
