const BASE_DATE_MS = Date.UTC(2000, 0, 1, 0, 0, 0);
const BASE = 26;
const CHAR_CODE_A = 65;

function secondsSinceBase(date = new Date()) {
    const timestamp = date instanceof Date ? date.getTime() : new Date(date).getTime();
    return Math.max(0, Math.floor((timestamp - BASE_DATE_MS) / 1000));
}

function encodeBase26(value) {
    if (value <= 0) {
        return 'A';
    }
    let result = '';
    let current = value;
    while (current > 0) {
        const remainder = current % BASE;
        result = String.fromCharCode(CHAR_CODE_A + remainder) + result;
        current = Math.floor(current / BASE);
    }
    return result;
}

function computeVersionId(date = new Date()) {
    const seconds = secondsSinceBase(date);
    return encodeBase26(seconds);
}

export {
    computeVersionId
};
