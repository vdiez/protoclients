let path = require('path');
let fs = require('fs-extra');

function load_protocol(protocol) {
    return fs.readdir(path.posix.join(__dirname, './protocols'))
        .then(files => {
            if (!files.includes(protocol + ".js")) throw "Incorrect protocol";
            return require('./protocols/' + protocol);
        });
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
    return load_protocol(protocol).then(client => client(params, log));
}
module.exports.parameters = protocol => load_protocol(protocol).then(client => client.parameters)

module.exports.get_class = protocol => load_protocol(protocol)