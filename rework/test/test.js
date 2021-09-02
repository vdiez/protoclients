const ftpServer = require('./ftp/server.js');
const ProtoClient = require('../index.js');

const host = '127.0.0.1';
const port = '9876';
const user = 'anonymous';
const password = 'anonymous@';

const localDirectory = './rework/test/local/';

// start ftp server

const main = async () => {
    await ftpServer({ host, port });
    const ftpClient = ProtoClient('FTP', { host, port });
    /*  await ftpClient.remove('/');
    await ftpClient.mkdir('/aze/test'); */
    //await ftpClient.mkdir('/aze/test2');
    const list = await ftpClient.stat('ftp.txt');
    console.log(list);
    const copy = await ftpClient.copy('ftp.txt', localDirectory + 'ftp_copy.txt');
    console.log(copy);
};

main();