const {createClient, AuthType} = require('webdav');
const base = require('../base');

const auth = {none: AuthType.None, basic: AuthType.Password};

module.exports = class extends base {
    static parameters = {
        ...base.parameters,
        parallel: {type: 'number', max: 50},
        host: {type: 'text'},
        username: {type: 'text'},
        password: {type: 'secret'},
        auth_type: {type: 'selectbox', options: [{id: 'none', label: 'None'}, {id: 'basic', label: 'Basic'}]}
    };

    constructor(params, logger) {
        super(params, logger, 'webdav');
    }

    static generate_id(params) {
        return JSON.stringify({protocol: 'webdav', host: params.host, user: params.username, password: params.password});
    }

    connect(slot) {
        if (this.connections[slot]) {
            this.connections[slot] = createClient(this.host, {username: this.params.username, password: this.params.password, authType: auth[this.params.auth_type] || AuthType.None});
            this.logger.info(`WebDAV (slot ${slot}) connection established with ${this.params.host}`);
        }
        return this.connections[slot];
    }

    createReadStream(source, options) {
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug(`WebDAV (slot ${slot}) create read stream from: `, source);
            const stream = connection.createReadStream(source, {range: {start: options?.start, end: options?.end}});
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('end', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }

    createWriteStream(target, options) {
        return this.wrapper((connection, slot, slot_control) => {
            this.logger.debug(`WebDAV (slot ${slot}) create write stream to: `, target);
            const stream = connection.createWriteStream(target, options);
            slot_control.keep_busy = true;
            stream.on('error', slot_control.release_slot);
            stream.on('finish', slot_control.release_slot);
            stream.on('close', slot_control.release_slot);
            return stream;
        }, true);
    }

    mkdir(dir) {
        if (dir === '.' || dir === '/' || dir === '') return;
        return this.wrapper((connection, slot) => {
            this.logger.debug(`WebDAV (slot ${slot}) mkdir: `, dir);
            return connection.createDirectory(dir, {recursive: true});
        });
    }

    read(filename, params = {}) {
        if (params.start || params.end) {
            this.logger.debug(`WebDAV download from: ${filename}`);
            return this.createReadStream(filename, params)
                .then(stream => this.constructor.get_data(stream, params.encoding));
        }
        return this.wrapper((connection, slot) => {
            this.logger.debug(`WebDAV (slot ${slot}) download from: `, filename);
            return connection.getFileContents(filename, params);
        });
    }

    stat(file) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`WebDAV (slot ${slot}) stat: `, file);
            return connection.stat(file)
                .then(stat => ({size: stat.size, mtime: new Date(stat.lastmod), isDirectory: () => stat.type === 'directory'}));
        });
    }

    write(target, contents = Buffer.alloc(0)) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`WebDAV (slot ${slot}) upload to: `, target);
            return connection.putFileContents(target, contents);
        });
    }

    copy(source, target) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`webDAV (slot ${slot}) copy ${source} to ${target}`);
            return connection.copyFile(source, target);
        });
    }

    remove(target) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`webDAV (slot ${slot}) remove: `, target);
            return connection.deleteFile(target);
        });
    }

    move(source, target) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`webDAV (slot ${slot}) move: `, source, target);
            return connection.moveFile(source, target);
        });
    }

    list_dir(dirname, params) {
        return this.wrapper((connection, slot) => {
            this.logger.debug(`webDAV (slot ${slot}) list: `, dirname);
            return this.getDirectoryContents(dirname, params);
        });
    }

    list(dirname) {
        this.list_dir(dirname)
            .then(list => {
                for (let i = 0; i < list.length; i++) {
                    list[i].isDirectory = () => list[i].type === 'directory';
                    list[i].mtime = new Date(list[i].lastmod);
                }
                return list;
            });
    }

    walk({dirname, ignored, on_file, on_error}) {
        this.list_dir(dirname, {deep: true})
            .then(list => {
                for (let i = 0; i < list.length; i++) {
                    if (!list[i].filename.match(ignored) && list[i].type === 'file') {
                        on_file(list[i].filename, {size: list[i].size, mtime: new Date(list[i].lastmod), isDirectory: () => false});
                    }
                }
            })
            .catch(on_error);
    }
};
