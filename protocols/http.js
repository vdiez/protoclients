const got = require('got');
const Base = require('../base');
const {rootDirnames} = require('../config');

class HTTP extends Base {
    static parameters = [];

    constructor(params, logger) {
        super('http', params, logger);
    }

    static generate_id() {
        return 'http';
    }

    createReadStream(source) {
        const control_release = true;
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`HTTP (slot ${slot}) create stream from: `, source);
            slot_control.keep_busy = true;
            const stream = got.stream(source, {
                retry: 0,
                https: {
                    rejectUnauthorized: false
                }
            });
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, control_release);
    }

    read(filename, params = {}) {
        return this.queue.run((slot) => {
            this.logger.debug(`HTTP (slot ${slot}) download from: `, filename);
            return got(filename, {
                retry: 0,
                https: {
                    rejectUnauthorized: false
                }
            }).then(response => Base.get_data(response.body, params.encoding));
        });
    }

    stat(filename) {
        return this.queue.run((slot) => {
            this.logger.debug(`HTTP (slot ${slot}) stat: `, filename);
            return got.head(filename, {
                retry: 0,
                https: {
                    rejectUnauthorized: false
                }
            });
        }).then(response => {
            const stats = {
                isDirectory: () => false
            };
            const headers = response.headers;
            if (headers) {
                if (headers['content-length']) {
                    stats.size = parseInt(headers['content-length'], 10);
                }
                if (headers['last-modified']) {
                    stats.mtime = new Date(headers['last-modified']);
                }
            }
            return stats;
        });
    }

    static normalize_path(uri, is_filename) {
        uri = Base.normalize_path(uri);
        if (rootDirnames.includes(uri)) {
            return '';
        }
        // What are those regex ?
        return uri.replace(/\/+$/, '').concat(is_filename ? '' : '/').replace(/^\/+/, '');
    }
}

module.exports = HTTP;
