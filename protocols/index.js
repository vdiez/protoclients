const AWS_S3 = require('./aws_s3');
const FS = require('./fs');
const FTP = require('./ftp');
const HTTP = require('./http');
const SMB = require('./smb');
const SSH = require('./ssh');

const protocols = {
    aws_s3: AWS_S3,
    fs: FS,
    ftp: FTP,
    http: HTTP,
    smb: SMB,
    ssh: SSH,
};

module.exports = protocols;
