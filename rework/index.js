const silentLogger = console;//{info() {}, warn() {}, error() {}, verbose() {}, debug() {}};

const _PROTOCOLS = {
    FTP : require('../protocols/ftp.js'),
    // FS : require('./fs.js'),
    // AWS : require('./aws_s3.js'),
    // HTTP : require('./http.js'),
    // SMB : require('./smb.js'),
    // SSH : require('./ssh.js'),
};

class ClientManager {

    constructor() {
        this.clients = [];
    }

    get(protocolName, params) {
        if(!protocolName) {
            throw 'No protocol'; 
        }
        const Protocol = _PROTOCOLS[protocolName];
        const clientId = Protocol.generate_id(params);
        return this.clients[clientId];
    }

    create(protocolName, params, logger) {
        // add method for logger if doesn't exist in logger param
        logger = { ...silentLogger, ...logger };
        // get constructor of defined protocol
        const Protocol = _PROTOCOLS[protocolName];
        if(!Protocol) {
            throw `Incorrect protocol ${protocolName}`;
        }
        try {
            const clientId = Protocol.generate_id(params);
            const client = new Protocol(params, logger);
            this.clients[clientId] = client;
            return client;
        } catch(e) {
            throw `Error on protoclients : ${e}`;
        }
    }

}
const clientsManager = new ClientManager();

module.exports = (...args) => clientsManager.get(...args) || clientsManager.create(...args);
module.exports.listProtocols = () => Object.keys(_PROTOCOLS);