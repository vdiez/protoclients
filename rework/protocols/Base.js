const silentLogger = { info() {}, warn() {}, error() {}, verbose() {}, debug() {} };

class Base {

    constructor(params, logger) {
        this.logger = logger || console || silentLogger;
    }

    static generateId() {
        return this.name || 'Base';
    }

}

module.exports = Base;