/**
 * Convert number to string
 * Add trailing zero to match the right length
 */
const zeroPad = (value, len) => String(value).padStart(len, '0');

/**
  * Return object with several formatted data
  */
const dateFormats = (date) => ({
    YYYY: zeroPad(date.getFullYear(), 4),
    MM: zeroPad(date.getMonth() + 1, 2),
    DD: zeroPad(date.getDate(), 2),
    HH: zeroPad(date.getHours(), 2),
    mm: zeroPad(date.getMinutes(), 2),
    ss: zeroPad(date.getSeconds(), 2),
    SSS: zeroPad(date.getMilliseconds(), 3)
});

/**
  * Equivalent of moment().format('YYYYMMDDHHmmssSSS')
  */
const YYYYMMDDHHmmssSSS = (date) => {
    const {YYYY, MM, DD, HH, mm, ss, SSS} = dateFormats(date);
    return `${YYYY}${MM}${DD}${HH}${mm}${ss}${SSS}`;
};

module.exports = {
    YYYYMMDDHHmmssSSS
};
