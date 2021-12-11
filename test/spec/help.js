const _slice = (xs, start, end) => xs.slice(start, end);
const _cmp = (x, y) => x < y ? -1 : (x > y ? 1 : 0);
const _isInt = Number.isInteger;
const _floor = Math.floor;
const _isInfP = x => x === Infinity;
const trunc = Math.trunc;
const id = x => x;



const _random = (min, max) => {
    if (max == null) { max = min; min = 0; }
    if (!(_isInt(min) && _isInt(max))) return NaN;
    if (min > max) { const temp = min; min = max; max = temp; }
    return min + _floor((max - min + 1) * Math.random());
};

const _randoms = (n, min, max, fn) => {
    if (max == null) { max = min; min = 0; }
    n = trunc(n);
    if (!n || n < 0 || _isInfP(n)) return [];
    if (!(_isInt(min) && _isInt(max))) return NaN;
    if (min > max) { const temp = min; min = max; max = temp; }
    const res = Array(n);
    let ind = 0;

    while(ind < n) { res[ind++] = min + fn((max - min + 1) * Math.random()); }
    return res;
};

const sortJS = (xs, fn, res = _slice(xs)) => (res.sort(fn), res);



export const len = xs => xs.length;
export const each = fn => xs => xs.forEach(fn);
export const random = (min, max) => _random(min, max);
export const randoms = (n, min, max) => _randoms(n, min, max, _floor);
export const randomsFloat = (n, min, max) => _randoms(n, min, max, id)
export const sort = xs => sortJS(xs, _cmp);
