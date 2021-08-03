const globals = {
    rootDirnames: ['.', '/', './', ''],
    timeoutConnection: 5 * 60 * 1000,
};

const awsConfig = {
    maxFileLength: 5 * 1024 * 1024 * 1024, // 5.368.709.120,
    maxTrials: 3,
    minPartSize: 50 * 1024 * 1024, // 52.428.800
    maxParts: 10000,
    basePartSize: 50 * 1024 * 1024, // 52.428.800
    concurrency: 8
};

const watcherConfig = {
    mode: 0o2775,
    ignored: /(^|[/\\])\../
};

module.exports = {
    ...globals,
    awsConfig,
    watcherConfig
};
