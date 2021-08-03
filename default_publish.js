const default_publish = (stream, size, publish) => {
    if (typeof publish !== 'function') return;
    let transferred = 0;
    let percentage = 0;
    stream.on('data', data => {
        transferred += data.length;

        const tmp = Math.round((transferred * 100) / size);
        if (percentage !== tmp) {
            percentage = tmp;
            publish({
                current: transferred,
                total: size,
                percentage
            });
        }
    });
};

module.exports = default_publish;
