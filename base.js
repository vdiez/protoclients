// Can we remove posix ?
const path = require('path').posix;
const get_stream = require('get-stream');
const is_stream = require('is-stream');
const parallel_limit = require('parallel_limit');
const Watcher = require('./watcher');
const {rootDirnames, timeoutConnection} = require('./config');

class Base {
    static accept_ranges = true;
    static parameters = {
        parallel: {
            number: true
        },
        polling: {
            boolean: true
        },
        polling_interval: {
            number: true
        }
    };

    constructor(protocol, params, logger) {
        this.logger = logger;
        this.protocol = protocol;
        this.params = {};
        this.disconnect_timeout = [];
        this.queue = parallel_limit(params.parallel);
        this.connections = new Array(params.parallel);
        this.update_settings(params);
    }

    id() {
        return this.generate_id(this.params);
    }

    update_settings(params) {
        this.params = params;
        this.queue.set_size(params.parallel);
    }

    wrapper(f, control_release) {
        return this.queue.run((slot, slot_control) => {
            clearTimeout(this.disconnect_timeout[slot]);
            return this.connect(slot).then(connection => f(connection, slot, slot_control).then(result => {
                const disconnect = () => {
                    this.disconnect_timeout[slot] = setTimeout(() => {
                        this.disconnect(slot);
                    }, timeoutConnection);
                };
                if (control_release && slot_control?.keep_busy) {
                    slot_control.release_promise.then(disconnect);
                } else {
                    disconnect();
                }
                return result;
            }));
        }, control_release);
    }

    create_watcher(params) {
        return new Watcher(params, this.logger, this);
    }

    connect() {
        throw {
            message: `connect method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    disconnect() {
        throw {
            message: `disconnect method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    static get_filename(dirname, uri) {
        return rootDirnames.includes(dirname)
            ? uri.replace(/^[/]+/, '')
            : uri.slice(dirname.length).replace(/^\/+/, '');
    }

    static normalize_path(dirname = '') {
        // What those regex are supposed to do ?
        const normalized = path.normalize(dirname);
        const removedDoubleSlashes = normalized.replace(/[\\/]+/g, '/');
        const removedTrailingSlashes = removedDoubleSlashes.replace(/^(.+?)\/*?$/, '$1'); // remove trailing slashes unless it's root path
        return removedTrailingSlashes;
    }

    static get_data(data, encoding = 'utf-8') {
        return Promise.resolve().then(() => {
            if (is_stream.readable(data) && typeof data !== 'string') {
                return get_stream.buffer(data);
            }
            return data;
        }).then(data => (encoding ? data.toString(encoding) : data));
    }

    createReadStream() {
        throw {
            message: `createReadStream method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    createWriteStream() {
        throw {
            message: `createWriteStream method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    mkdir() {
        throw {
            message: `mkdir method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    stat() {
        throw {
            message: `stat method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    read() {
        throw {
            message: `read method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    write() {
        throw {
            message: `write method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    copy() {
        throw {
            message: `copy method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    link() {
        throw {
            message: `link method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    symlink() {
        throw {
            message: `symlink method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    move() {
        throw {
            message: `move method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    remove() {
        throw {
            message: `remove method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    tag() {
        throw {
            message: `tag method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    list() {
        throw {
            message: `list method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }

    walk() {
        throw {
            message: `walk method not implemented for ${this.protocol}`,
            not_implemented: 1
        };
    }
}

module.exports = Base;
