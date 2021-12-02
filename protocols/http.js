const got = require('got');
const base = require('../base');

module.exports = class extends base {
    static parameters = [];

    constructor(params, logger) {
        super(params, logger, 'http');
    }

    static generate_id() {
        return 'http';
    }

    createReadStream(source) {
        return this.queue.run((slot, slot_control) => {
            this.logger.debug(`HTTP (slot ${slot}) create stream from: `, source);
            const stream = got.stream(source, {retry: 0, https: {rejectUnauthorized: false}});
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }

    read(filename, params = {}) {
        return this.queue.run(slot => {
            this.logger.debug(`HTTP (slot ${slot}) download from: `, filename);
            return got(filename, {retry: 0, https: {rejectUnauthorized: false}}).then(response => this.constructor.get_data(response.body, params.encoding));
        });
    }

    stat(filename) {
        return this.queue.run(slot => {
            this.logger.debug(`HTTP (slot ${slot}) stat: `, filename);
            return got.head(filename, {retry: 0, https: {rejectUnauthorized: false}});
        })
            .then(response => {
                const stats = {isDirectory: () => false};
                const headers = response.headers;
                if (headers && headers['content-length']) stats.size = parseInt(headers['content-length'], 10);
                if (headers && headers['last-modified']) stats.mtime = new Date(headers['last-modified']);
                return stats;
            });
    }

    static normalize_path(uri, is_filename) {
        uri = super.normalize_path(uri);
        if (uri === '.' || uri === '/' || uri === './' || uri === '') return '';
        return uri.replace(/\/+$/, '').concat(is_filename ? '' : '/').replace(/^\/+/, '');
    }
};
