let path = require('path');
let fs = require('fs-extra');
let protocols = [];

function load() {
    return fs.readdir(path.posix.join(__dirname, './protocols')).then(files => protocols = files);
}
function load_protocol(protocol) {
    if (!protocols.includes(protocol + ".js")) throw "Incorrect protocol or module not initialized";
    return require('./protocols/' + protocol);
}

module.exports = ({logger, protocol, params} = {}) => {
    let log = {info() {}, warn() {}, error() {}, verbose() {}, debug() {}};
    if (logger) {
        if (typeof logger.info === "function") log.info = logger.info;
        if (typeof logger.warn === "function") log.warn = logger.warn;
        if (typeof logger.error === "function") log.error = logger.error;
        if (typeof logger.debug === "function") log.debug = logger.debug;
        if (typeof logger.verbose === "function") log.verbose = logger.verbose;
    }
    return load_protocol(protocol).then(client => new client(params, log));
}
module.exports.parameters = protocol => load_protocol(protocol).then(client => client.parameters)

module.exports.load_protocols = load;

module.exports.list = () => protocols.reduce((result, protocol) => {
    let extension = path.posix.extname(protocol);
    protocol = protocol.slice(0, -extension.length);
    result[protocol] = require('./protocols/' + protocol);
    return result;
}, {});

module.exports.get_class = protocol => load_protocol(protocol)