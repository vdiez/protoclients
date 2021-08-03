const PROTOCOLS = require('./protocols');

const silentLogger = {
    info() {}, warn() {}, error() {}, verbose() {}, debug() {}
};

// THIS IS USELESS NOW, WE NEED TO REMOVE THIS FROM OTHER PROJECTS
// WE CAN KEEP METHOD BUT NOT EXPORTING IT
const get_class = protocol => {
    if (!PROTOCOLS[protocol]) {
        throw 'Incorrect protocol';
    }
    return PROTOCOLS[protocol];
};

const are_equal = (params1, params2) => get_class(params1.protocol).generate_id(params1) === get_class(params2.protocol).generate_id(params2);

const list = () => PROTOCOLS;

// THIS IS USELESS TOO
const parameters = protocol => get_class(protocol).parameters;

const clients = {};
const main = ({logger, protocol, params} = {}) => {
    // Merge two logger, logger > silentLogger
    const log = {...silentLogger, ...logger};
    const Protocol = get_class(protocol);
    const client_id = Protocol.generate_id(params);
    if (!clients[client_id]) {
        clients[client_id] = new Protocol(params, log);
    }
    return clients[client_id];
};

module.exports = main;
module.exports = {
    are_equal,
    get_class,
    list,
    parameters,
};
