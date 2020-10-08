let got = require('got');
let base = require("../base");

module.exports = class extends base {
    static parameters = [];
    constructor(params, logger) {
        super(params, logger, "http");
    }
    static generate_id(params) {
        return 'http';
    }
    update_settings(params) {
        super.update_settings(params);
    }
    createReadStream(source) {
        return this.queue.run(() => got.stream(source, {retry: 0, https: {rejectUnauthorized: false}}));
    }
    read(filename, params = {}) {
        return this.queue.run(() => got(filename, {retry: 0, https: {rejectUnauthorized: false}}).then(response => this.constructor.get_data(response.body, params.encoding)));
    }
    stat(filename) {
        return this.queue.run(() => got.head(filename, {retry: 0, https: {rejectUnauthorized: false}})).then(response => {
            let stats = {isDirectory: () => false};
            let headers = response.headers;
            if (headers && headers['content-length']) stats.size = parseInt(headers['content-length'], 10);
            if (headers && headers['last-modified']) stats.mtime = new Date(headers['last-modified']);
            return stats;
        });
    }
    static normalize_path(dirname, is_filename) {
        return dirname.replace(/[\\\/]+/g, "/").replace(/\/+$/, "").concat(is_filename ? "" : "/").replace(/^\/+/, "")
    }
}
