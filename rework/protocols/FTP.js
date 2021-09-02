const Base = require('./Base.js');
const path = require('path');
const basicFtp = require('basic-ftp');
const fs = require('fs');
const Client = basicFtp.Client;

const _PROTOCOL = 'ftp';
const _TIMEOUT = 5 * 60 * 1000;

// https://github.com/patrickjuchli/basic-ftp/blob/master/src/FileInfo.ts
const FileType = [
    'Unknown',
    'File',
    'Directory',
    'SymbolicLink'
];


class FTP extends Base {
  
    constructor(params, logger) {
        super(params, logger);
        const { host, user, password, port, secure } = params;
        
        this.timeout;

        this.protocol = _PROTOCOL;
        this.host = host;
        this.port = port;
        this.secure = secure;
        this.user = user;
        this.password = password;

        this.client = new Client();
        this.client.prepareTransfer = basicFtp.enterPassiveModeIPv4;

    }

    static generateId(params) {
        if(!params) {
            this.logger.log('No params');
        }
        const { host, user, password, port } = params;
        return JSON.stringify({
            protocol: _PROTOCOL, 
            host,
            user,
            password,
            port,
            secure: false
        });
    }

    url() {
        return `ftp://${this.host}:${this.port}`;
    }

    disconnect() {
        console.log('Disconnection due to timeout');
        this.client.close();
        clearTimeout(this.timeout);
    }

    async connect() {
        if(!this.client) {
            this.logger.log('Client should be instantiate to be connected');
        }
        if(!this.client.closed) {
            return;
        }
        console.log(`Connection established to ${this.url()}`);
        await this.client.access({
            host: this.host,
            user: this.user,
            password: this.password,
            port: this.port,
            secure: this.secure,
            secureOptions: {
                rejectUnauthorized: false
            }
        });
        this.timeout = setTimeout(() => {
            this.disconnect();
        }, _TIMEOUT);
    }

    async init() {
        await this.connect();
        await this.client.cd('.');
    }

    async mkdir(path) {
        try {
            await this.init();
            return this.client.ensureDir(path);
        } catch(e) {
            this.logger.log(`Error when executing "mkdir" : path "${path}" already exists`);
        }
    }

    async remove(path) {
        try {
            await this.init();
            return this.client.removeDir(path);
        } catch(e) {
            this.logger.log(`Error when executing "remove" : path "${path}" doesn't exist`);
        }
    }

    async list(path) {
        try {
            await this.init();
            return this.client.list(path);
        } catch(e) {
            this.logger.log(`Error when executing "list" : ${e}`);
        }
    }
    
    async stat(path) {
        await this.init();
        const filesInfo = await this.list(path);
        const fileName = path.split('/').pop();
        const file = filesInfo.find(fileInfo => fileInfo.name === fileName && fileInfo.type === FileType['File']);
        if(file) {
            return {
                size: file.size,
                mtime: file.modifiedAt,
                isDirectory: FileType[file.type] === 'Directory'
            };
        } else {
            return filesInfo.map((file) => {
                return {
                    name: file.name,
                    size: file.size,
                    mtime: file.modifiedAt,
                    isDirectory: FileType[file.type] === 'Directory'
                };
            });
        }
    }

    async copy(source, target) {
        try {
            await this.init();
            const dirname = path.dirname(target);
            const filename = path.basename(target);
            if(!fs.existsSync(target)) {
                fs.mkdirSync(dirname, { recursive: true });
                fs.writeFileSync(target, '');
            }
            console.dir({ dirname, filename });
            const stream = fs.createWriteStream(target);
            await this.client.downloadTo(stream, source);
            return stream;
        } catch(e) {
            this.logger.log(`Error when executing "copy" : ${e}`);
        }
    }

    // read
    // write
    // copy
    // remove
    // move
    // walk

}

module.exports = FTP;