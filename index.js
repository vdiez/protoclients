const aws_s3 = require('./protocols/aws_s3');
const fs = require('./protocols/fs');
const ftp = require('./protocols/ftp');
const http = require('./protocols/http');
const smb = require('./protocols/smb');
const ssh = require('./protocols/ssh');
const webdav = require('./protocols/webdav');

const silentLogger = {info() {}, warn() {}, error() {}, verbose() {}, debug() {}, silly() {}};
const protocols = {aws_s3, fs, ftp, http, smb, ssh, webdav};
const clients = {};

const get_class = protocol => {
    if (!protocols.hasOwnProperty(protocol?.toLowerCase())) throw 'Incorrect protocol';
    return protocols[protocol];
};

module.exports = ({logger, protocol, params} = {}) => {
    const log = {...silentLogger, ...logger};
    const Protoclient = get_class(protocol || params?.protocol);
    const client_id = Protoclient.generate_id(params);
    if (!clients.hasOwnProperty(client_id)) clients[client_id] = new Protoclient(params, log);
    return clients[client_id];
};

module.exports.parameters = protocol => get_class(protocol).parameters;

module.exports.list = protocols;

module.exports.get_class = get_class;

module.exports.are_equal = (params1, params2) => get_class(params1.protocol).generate_id(params1) === get_class(params2.protocol).generate_id(params2);
