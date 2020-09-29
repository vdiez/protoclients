let path = require('path');
let fs = require('fs-extra');
let protocols = fs.readdirSync(path.posix.join(__dirname, './protocols'));

module.exports = ({logger, protocol, params} = {}) => {
    let log = {info() {}, warn() {}, error() {}, verbose() {}, debug() {}};
    if (logger) {
        if (typeof logger.info === "function") log.info = logger.info;
        if (typeof logger.warn === "function") log.warn = logger.warn;
        if (typeof logger.error === "function") log.error = logger.error;
        if (typeof logger.debug === "function") log.debug = logger.debug;
        if (typeof logger.verbose === "function") log.verbose = logger.verbose;
    }
    if (!protocols.includes(protocol + ".js")) throw "Incorrect protocol";
    return new (require('./protocols/' + protocol))(params, log);
}
module.exports.parameters = protocol => {
    if (!protocols.includes(protocol + ".js")) throw "Incorrect protocol";
    return require('./protocols/' + protocol).parameters();
}

module.exports.list = () => protocols.reduce((result, protocol) => {
    let extension = path.posix.extname(protocol);
    protocol = protocol.slice(0, -extension.length);
    result[protocol] = require('./protocols/' + protocol);
    return result;
}, {});

module.exports.get_class = protocol => {
    if (!protocols.includes(protocol + ".js")) throw "Incorrect protocol";
    return require('./protocols/' + protocol);
}