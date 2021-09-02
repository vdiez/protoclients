const FtpSrv = require('ftp-srv');

const startServer = async ({ host, port }) => {
    
    const ftpServer = new FtpSrv({ 
        url :  `ftp://0.0.0.0:${port}`, 
        anonymous: true, 
        pasv_url: `ftp://${host}` 
    });
    ftpServer.on('login', ({ connection }, resolve, reject) => {
        connection.on('RETR', (error, filePath) => {
            console.log('RETR' + filePath);
        });
        connection.on('STOR', (error, fileName) => {
            console.log('STOR' + fileName);
        });
        try {
            resolve({
                root: './rework/test/ftp/resources/'
            });
        } catch(e) {
            reject();
        }
    });
    ftpServer.on('client-error', ({ context, error}) => { 
        console.log('client-error', context, error);
    });
    await ftpServer.listen();
    console.log(`FTP server listening on ftp://${host}:${port}`);
};


module.exports = startServer;