const _slice = (xs, start, end) => xs.slice(start, end);
const init = xs => _slice(xs, 0, -1);
const isArray = Array.isArray;
const append = (xs, ys) => xs.concat(ys);
const isString = x => typeof x === 'string';
const isNumber = x => x === x && typeof x === 'number';
const isFunction = x => typeof x === 'function';
const isObject = x => Object.prototype.toString.call(x) === '[object Object]';
const keys = Object.keys;
const len = xs => xs.length;
const nul = xs => len(xs) === 0;
const trim = str => str.trim();
const take = (n, xs) => _slice(xs, 0, n < 0 ? 0 : n);
const drop = (n, xs) => _slice(xs, n < 0 ? 0 : n);
const copy = xs => _slice(xs);
const last = xs => xs[len(xs) -1];
const extend = (obj1, obj2) => Object.assign({}, obj1, obj2);
const toLower = str => isString(str) ? str.toLowerCase() : str;
const floor = Math.floor;
const ceil = Math.ceil;
const minFn = (x, y) => y < x ? y : x;
const maxFn = (x, y) => y > x ? y : x;

const isElem = (e, xs) => xs.indexOf(e) > -1;

const _set = (xs, ys, bool) => {
    const res = new Set();
    const set = new Set(ys);
    for(let elem of xs) if (set.has(elem) === bool) res.add(elem);
    return [...res];
};

const intersect = (xs, ys) => _set(xs, ys, true);
const difference = (xs, ys) => _set(xs, ys, false);
const isUniq = xs => new Set(xs).size === len(xs);

const isSubset = (sub, xs) => {
    const superSet = new Set(xs);
    const size = superSet.size;
    for (let item of sub) superSet.add(item);
    return size === superSet.size;
};

const any = pred => xs => xs.some(pred);
const all = pred => xs => xs.every(pred);
const map = fn => xs => xs.map(fn);
const each = fn => xs => xs.forEach(fn);
const filter = pred => xs => xs.filter(x => pred(x));

const mapKeys = (fn, keys) => {
    const res = Object.create(null);
    for (let key of keys) res[key] = fn(key);
    return res;
};

const concat = (...xss) => {
    const res = [];
    for (let xs of xss) {
        for (let item of xs)
            res.push(item);
    }
    return res;
};

const pick = keys => obj => {
    const res = {};
    for (let key of keys) res[key] = obj[key];
    return res;
};

const select = keys => objs => map (pick(keys)) (objs);

const _pick = keys => obj => {
    const res = {};
    for (let key of keys) res[key] = obj[key] == null ? null : obj[key];
    return res;
};

const _select = keys => objs => map (_pick(keys)) (objs);


const pickTS = tss => obj => {
    const res = {};
    for (let keys of tss) res[keys[0]] = obj[keys[1]] == null ? null : obj[keys[1]];
    return res;
};

const pick2 = (keys1, keys2, fn) => (obj1, obj2) => {
    const res = {};
    for (let key of keys1) res[key] = obj1[key] == null ? null : obj1[key];
    for (let key of keys2) res[key] = obj2[key] == null ? null : obj2[key];
    if (fn) fn(res, obj1, obj2);
    return res;
};


const pick2KTS = (keys, tss, fn) => (obj1, obj2) => {
    const res = {};
    for (let key of keys) res[key] = obj1[key] == null ? null : obj1[key];
    for (let keys of tss) res[keys[0]] = obj2[keys[1]] == null ? null : obj2[keys[1]];
    if (fn) fn(res, obj1, obj2);
    return res;
};

const pick2TSK = (tss, keys, fn) => (obj1, obj2) => {
    const res = {};
    for (let keys of tss) res[keys[0]] = obj1[keys[1]] == null ? null : obj1[keys[1]];
    for (let key of keys) res[key] = obj2[key] == null ? null : obj2[key];
    if (fn) fn(res, obj1, obj2);
    return res;
};

const pick2TS = (tss1, tss2, fn) => (obj1, obj2) => {
    const res = {};
    for (let keys of tss1) res[keys[0]] = obj1[keys[1]] == null ? null : obj1[keys[1]];
    for (let keys of tss2) res[keys[0]] = obj2[keys[1]] == null ? null : obj2[keys[1]];
    if (fn) fn(res, obj1, obj2);
    return res;
};

const equalBy = fn => (x, y) => fn(x) === fn(y);

const _nubSorted = (xs, eq) => {
    const xlen = len(xs);
    if (!xlen) return []; 
    const res = [];
    let ind = 0;
    let first = res[len(res)] = xs[ind++];

    while (ind < xlen) {
        first = eq(first, xs[ind]) ? first : res[len(res)] = xs[ind];
        ind++;
    }
    return res;
};

const nubSBy = (fn, xs) => _nubSorted(xs, fn);

const cmp_nul = (x, y, cmp) => x === y ? 0 : x == null ? -1 : y == null ? 1 : cmp(x, y);
const cmp2 = (x, y) => x < y ? -1 : 1;
const cmp3 = (x, y) => x.localeCompare(y);

const ascn = fn => (x, y) => cmp_nul (fn(x), fn(y), cmp2);
const descn = fn => (x, y) => -cmp_nul (fn(x), fn(y), cmp2);
const ascn_loc = fn => (x, y) => (x = fn(x), cmp_nul (x, fn(y), isString(x) ? cmp3 : cmp2));
const descn_loc = fn => (x, y) => (x = fn(x), -cmp_nul (x, fn(y), isString(x) ? cmp3 : cmp2));



class Node {
    constructor(color, value){
        this.color = color;
        this.value = value;
        this.left = null;
        this.right = null;
    }
}

const RED = true;
const BLACK = false;

class Tree {
    constructor() {
        this.root = null;
        this.compare = (x, y) => (x < y ? -1 : (x > y ? 1 : 0));
        this.map = {};
    }

    isRed (node) { return node != null && node.color === RED; }
    isBlack(node) { return node != null && node.color === BLACK; }

    rotateLeft(node) {
        const tmp = node.right;
        node.right = tmp.left;
        tmp.left = node;
        tmp.color = node.color;
        node.color = RED;
        return tmp;
    }

    rotateRight(node) {
        const tmp = node.left;
        node.left = tmp.right;
        tmp.right = node;
        tmp.color = node.color;
        node.color = RED;
        return tmp;
    }

    colorFlip(node) {
        node.color = !node.color;
        node.left.color = !node.left.color;
        node.right.color = !node.right.color;
    }

    max(node) {
        while (node.right != null) node = node.right;
        return node;
    }
    min(node) {
        while (node.left != null) node = node.left;
        return node;
    }

    get maxValue () { return this.root != null ? this.max(this.root).value : null; }
    get minValue () { return this.root != null ? this.min(this.root).value : null; }

    moveRedLeft(node) {
        this.colorFlip(node);
        if (this.isRed(node.right.left)) {
            node.right = this.rotateRight(node.right);
            node = this.rotateLeft(node);
            this.colorFlip(node);
        }
        return node;
    }

    moveRedRight(node) {
        this.colorFlip(node);
        if (this.isRed(node.left.left)) {
            node = this.rotateRight(node);
            this.colorFlip(node);
        }
        return node;
    }

    fixUp(node) {
        if (this.isRed(node.right)) node = this.rotateLeft(node);
        if (this.isRed(node.left) && this.isRed(node.left.left)) node = this.rotateRight(node);
        if (this.isRed(node.left) && this.isRed(node.right)) this.colorFlip(node);
        return node;
    }

    deleteMin(node) {
        if (node.left == null) return null;
        if (this.isBlack(node.left) && !this.isRed(node.left.left)) node = this.moveRedLeft(node);
        node.left = this.deleteMin(node.left);
        return this.fixUp(node);
    }

    _insert(node, value) {
        if (node == null) return new Node(RED, value);    
        const cmp = this.compare(value, node.value);
        if      (cmp < 0) node.left  = this._insert(node.left,  value);
        else if (cmp > 0) node.right = this._insert(node.right, value);
        else node.value = value;
        return this.fixUp(node);
    }

    _delete(node, value) {
        if (node == null) return null;
        let cmp = this.compare(value, node.value);
        if (cmp < 0) {
            if (this.isBlack(node.left) && !this.isRed(node.left.left)) node = this.moveRedLeft(node);
            node.left = this._delete(node.left, value);
        }
        else {
            if (this.isRed(node.left)) node = this.rotateRight(node);
            if (cmp === 0 && node.right == null) return null;
            if (this.isBlack(node.right) && !this.isRed(node.right.left)) node = this.moveRedRight(node);
            cmp = this.compare(value, node.value);
            if (cmp === 0) {
                node.value = this.min(node.right).value;
                node.right = this.deleteMin(node.right);
            }
            else node.right = this._delete(node.right, value);
        }
        return this.fixUp(node);
    }

    insert(value) {
        this.map[value] = (this.map[value] || 0) + 1;
        if (this.map[value] === 1) {
            this.root = this._insert(this.root, value);
            this.root.color = BLACK;
        }
    }

    remove(value) {
        if (this.map[value] === 1) {
            this.map[value] = 0;
            this.root = this._delete(this.root, value);
            if (this.root != null) this.root.color = BLACK;
        }
        else this.map[value]--;
    }
}

class SqlSelectError extends Error {}
const _throw = str => { throw new SqlSelectError(str); };

const _prodSQL = ({ fn, t1, t2 }) => {
    const xlen = len(t1);
    const ylen = len(t2);
    const res = Array(xlen * ylen);
    let idx = 0;
    let indx = -1;
    let indy;

    while (++indx < xlen) {
        indy = -1;
        while (++indy < ylen) {
            res[idx++] = fn(t1[indx], t2[indy]);
        }
    }
    return res;
};


const buildHash = (xs, str, map) => {
    let key, value;

    for (let item of xs) {
        key = item[str];
        if (key != null) {
            if (map.has(key)) {
                value = map.get(key);
                if (isArray(value)) value.push(item);
                else map.set(key, [value, item]);
            }
            else map.set(key, item);
        }
    }
};

const _hashjoin = (fn, t1, t2, str1, str2, left, right, tgs1, tgs2) => {
    const map = new Map();
    buildHash(t2, str2, map);
    const res = [];
    let match, nulls1, nulls2, key, set;

    if (left) nulls2 = mapKeys(_ => null, tgs2);
    if (right) { nulls1 = mapKeys(_ => null, tgs1); set = new Set(); }

    for (let item of t1) {
        key = item[str1];
        if (!map.has(key)) { if (left) res.push(fn(item, nulls2)); continue; }
        if (right) set.add(key);
        match = map.get(key);
        if (!isArray(match)) { res.push(fn(item, match)); continue; }
        for (let _item of match) res.push(fn(item, _item));
    }

    if (right) {
        for (let item of t2) {
            key = item[str2];
            if (!set.has(key)) res.push(fn(nulls1, item));
        }
    }
    return res;
};

const buildSet = (xs, str, set) => {
    let key;
    for (let item of xs) {
        key = item[str];
        if (key != null) set.add(key);
    }
};

const _semijoin = (fn, t1, t2, str1, str2, semi) => {
    const set = new Set();
    buildSet(t2, str2, set);
    const res = [];
    let match, key;

    for(let item of t1) {
        key = item[str1];
        match = set.has(key);
        if (semi) { if (match) res.push(fn(item)); }
        else { if (!match) res.push(fn(item)); }     
    }
    return res;
};

const cmp = cmps => (x, y) => {
    const clen = len(cmps);
    let res = 0;
    let ind = 0;

    while (res === 0 && ind < clen) res = cmps[ind++](x, y);
    return res;
};

const _eqs = eqs => (x, y) => {
    const elen = len(eqs);
    let res = true;
    let ind = 0;

    while (res && ind < elen) res = eqs[ind++](x, y);
    return res;
};

const _flip = fn => (obj1, obj2) => fn(obj2, obj1);

const flip = ({ fn, t1, t2, str1, str2, tgs1, tgs2 }, b1, b2) =>
    len(t1) >= len(t2) ? _hashjoin(fn, t1, t2, str1, str2, b1, b2, tgs1, tgs2) :
                         _hashjoin(_flip(fn), t2, t1, str2, str1, b2, b1, tgs2, tgs1);


const hashjoin = obj => flip(obj, false, false);
const leftjoin = obj => flip(obj, true, false);
const rightjoin = obj => flip(obj, false, true);
const fulljoin = obj => flip(obj, true, true);

const leftsemijoin = ({fn, t1, t2, str1, str2}) => _semijoin(fn, t1, t2, str1, str2, true);
const rightsemijoin = ({fn, t1, t2, str1, str2}) => _semijoin(fn, t2, t1, str2, str1, true);
const leftantijoin = ({fn, t1, t2, str1, str2}) => _semijoin(fn, t1, t2, str1, str2);
const rightantijoin = ({fn, t1, t2, str1, str2}) => _semijoin(fn, t2, t1, str2, str1);

const avgFn = (arr, AS) => {
    for(let ind = 0; ind < arr.length; ind++)
        arr[ind][AS] = arr[ind][AS] && arr[ind][AS]['count'] ? arr[ind][AS]['sum'] / arr[ind][AS]['count'] : null;
};

const sumFn = (arr, AS, nullValue) => {
    for(let ind = 0; ind < arr.length; ind++)
        arr[ind][AS] = arr[ind][AS] && arr[ind][AS]['count'] ? arr[ind][AS]['sum'] : nullValue;    
};


const executeUnordered = (partitions, obj, arr, AS) => {
    const plen = len(partitions); 
    let pfirst = 0;
    let ind = -1;
    let partition, value, idx;

    obj.fromWin = true;
    while (++ind < plen) {
        partition = partitions[ind];
        value = obj['fn'](null, arr, pfirst, partition);
        idx = -1;
        while (++idx < partition) arr[pfirst + idx][AS] = value;
        pfirst += partition;
    }

    if (obj.avg) avgFn(arr, AS);
    if (obj.sum) sumFn(arr, AS, null);
};

const exNulls = (obj, arr, AS) => {
    const nullValue = obj.count ? 0 : null;
    const alen = len(arr);
    let ind = -1;

    while (++ind < alen) arr[ind][AS] = nullValue;
};


const executeRows = (partitions, obj, arr, AS, up, uf, offset1, offset2) => {
    const plen = len(partitions);
    const neg1 = offset1 <= 0;
    const neg2 = offset2 <= 0;
    const min = obj.min;
    const max = obj.max;
    const nullValue = obj.count ? 0 : null;
    let pstart = -1;
    let pfirst = 0;
    let ind = -1;
    let partition, plast, pend, res, idx, index, fb, broken, tree, rest, init, toRem, toIns;

    obj.fromWin = true;
    while (++ind < plen) {
        partition = partitions[ind];
        pend = pfirst + partition;
        plast = pend - 1;
        broken = false;
        init = true;
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            if (up && index + offset1 < pfirst) {
                fb = index + offset2;
                if (neg2) {
                    res = fb < pfirst ? nullValue : obj.fn(null, arr, fb, 1);
                    arr[index][AS] = fb > pfirst ? obj.fnP(arr[index - 1][AS], res) : res;
                }
                else {
                    if (fb < pend) {
                        res = index === pfirst ? obj.fn(null, arr, pfirst, 1 + offset2) : obj.fn(null, arr, fb, 1);
                        arr[index][AS] = index > pfirst ? obj.fnP(arr[index - 1][AS], res) : res;
                    }
                    else arr[index][AS] = index > pfirst ? arr[index - 1][AS] : obj.fn(null, arr, index, partition);
                }
            }
            else if (uf && index + offset2 > plast) { broken = true; break; }
            else {
                if (init) {
                    init = false;
                    if (min || max) {
                        tree = new Tree();
                        obj.fn(null, arr, index + offset1, 1 + offset2 - offset1, tree);
                        arr[index][AS] = min ? tree.minValue : tree.maxValue;
                    }
                    else {
                        arr[index][AS] = index === pfirst ?
                                         obj.fn(null, arr, index + offset1, 1 + offset2 - offset1) :
                                         obj.fnP(arr[index - 1][AS], obj.fn(null, arr, index + offset2, 1));
                    }
                }
                else {
                    if (min || max) {
                        toRem = obj.fn(null, arr, index + offset1 - 1, 1);
                        if (toRem != null) tree.remove(toRem);
                        toIns = obj.fn(null, arr, index + offset2, 1);
                        if (toIns != null) tree.insert(toIns);
                        arr[index][AS] = min ? tree.minValue : tree.maxValue;
                    }
                    else {
                        rest = obj.fnM(arr[index - 1][AS], obj.fn(null, arr, index + offset1 - 1, 1));
                        arr[index][AS] = obj.fnP(rest, obj.fn(null, arr, index + offset2, 1));
                    }
                }
            }
            idx++;
        }

        if (broken) {
            pstart = pfirst - 1;
            idx = 0;
            while (idx < partition) {
                index = plast - idx;
                if (index + offset2 > plast) {
                    fb = index + offset1;
                    if (neg1) {
                        if (fb > pstart) {
                            res = obj.fn(null, arr, fb, 1 - (index === plast ? offset1 : 0));
                            arr[index][AS] = index < plast ? obj.fnP(arr[index + 1][AS], res) : res;
                        }
                        else arr[index][AS] = index < plast ? arr[index + 1][AS] : obj.fn(null, arr, pfirst, partition);
                    }
                    else {
                        res = fb > plast ? nullValue : obj.fn(null, arr, fb, 1);
                        arr[index][AS] = fb < plast ? obj.fnP(arr[index + 1][AS], res) : res;
                    }
                }
                else break;
                idx++;
            }
        }
        pfirst = pend;
    }

    if (obj.avg) avgFn(arr, AS);
    if (obj.sum) sumFn(arr, AS, nullValue);
};


const _bsgt = fn => (x, y) => x > (fn(y) == null ? -Infinity : fn(y));
const _bslt = fn => (x, y) => x < (fn(y) == null ? -Infinity : fn(y));


const bsLeft = (e, xs, low, high, fn) => {
    let mid;
    
    while (low < high) {
        mid = Math.floor(low + (high - low) / 2);
        if (fn(e, xs[mid])) low = mid + 1;
        else high = mid;
    }
    return low;
};

const bsRight = (e, xs, low, high, fn) => {
    let mid;
    
    while (low < high) {
        mid = Math.floor(low + (high - low) / 2);
        if (fn(e, xs[mid])) high = mid;
        else low = mid + 1;
    }
    return high;
};


const executeRange = (partitions, obj, arr, AS, up, uf, offset1, offset2, ordColumn) => {
    const plen = len(partitions);
    const min = obj.min;
    const max = obj.max;
    const nullValue = obj.count ? 0 : null;
    const fn = obj => obj[ordColumn];
    const desc = obj.desc;
    const ltfn = desc ? _bsgt(fn) : _bslt(fn);
    const gtfn = desc ? _bslt(fn) : _bsgt(fn);
    const _offset1 = offset1;
    const _offset2 = offset2;
    let pstart = -1;
    let pfirst = 0;
    let ind = -1;
    let partition, plast, pend, idx, index, broken, tree, init, target;
    let minInd, maxInd, value, curr, _minInd, _maxInd, rest, remInd, insInd;

    if (desc) { offset1 = -offset1; offset2 = -offset2; }

    obj.fromWin = true;
    while (++ind < plen) {
        partition = partitions[ind];
        pend = pfirst + partition;
        plast = pend - 1;
        broken = false;
        maxInd = pfirst;
        minInd = plast;
        init = true;
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            if (up && _offset1 === -Infinity) {
                if (_offset2 === 0) {
                    minInd = index;
                    maxInd = index;
                    curr = obj.fn(null, arr, index, 1);
                    target = index === pfirst ? curr : obj.fnP(arr[index - 1][AS], curr);
                    value = arr[index++][ordColumn];
                    while (index < pend && value === arr[index][ordColumn]) {
                        target = obj.fnP(target, obj.fn(null, arr, index, 1));
                        maxInd = index++;
                    }
                    while (minInd <= maxInd) arr[minInd++][AS] = target;
                    idx = index - pfirst;
                }
                else {
                    value = arr[index][ordColumn];
                    minInd = maxInd;
                    maxInd = bsRight(value != null ? value + offset2 : -Infinity, arr, pfirst, pend, ltfn);


                    if (maxInd === pfirst) arr[index][AS] = nullValue;
                    else if (minInd < maxInd) {
                        target = obj.fn(null, arr, minInd, maxInd - minInd);
                        arr[index][AS] = index === pfirst ? target : obj.fnP(arr[index - 1][AS], target);
                    }
                    else arr[index][AS] = arr[index - 1][AS];
                    idx++;
                }
            }

            else if (uf && _offset2 === Infinity) { broken = true; break; }
            else {
                value = arr[index][ordColumn];
                _minInd = minInd;
                _maxInd = maxInd;
                minInd = bsLeft(value != null ? value + offset1 : -Infinity, arr, pfirst, pend, gtfn);
                maxInd = bsRight(value != null ? value + offset2 : -Infinity, arr, pfirst, pend, ltfn);


                if (maxInd === pfirst || minInd === pend || minInd === maxInd) arr[index][AS] = nullValue;
                else if (init || _minInd === _maxInd) { 
                    init = false;
                    if (min || max) {
                        tree = new Tree();
                        obj.fn(null, arr, minInd, maxInd - minInd, tree);
                        arr[index][AS] = min ? tree.minValue : tree.maxValue;
                    }
                    else arr[index][AS] = obj.fn(null, arr, minInd, maxInd - minInd);
                }
                else {
                    remInd = minFn(_maxInd, minInd);
                    insInd = maxFn(_maxInd, minInd);
                    if (min || max) {
                        if (minInd > _minInd) obj.fn(null, arr, _minInd, remInd - _minInd, tree, true);
                        if (maxInd > _maxInd) obj.fn(null, arr, insInd, maxInd - insInd, tree);
                        arr[index][AS] = min ? tree.minValue : tree.maxValue;
                    }
                    else {
                        rest = minInd > _minInd ?
                            obj.fnM(arr[index - 1][AS], obj.fn(null, arr, _minInd, remInd - _minInd)) :
                            arr[index - 1][AS];
                
                        arr[index][AS] = maxInd > _maxInd ?
                            obj.fnP(rest, obj.fn(null, arr, insInd, maxInd - insInd)) : rest;
                    }
                }
                idx++;
            }
        }

        if (broken) {
            pstart = pfirst - 1;
            idx = 0;
            while (idx < partition) {
                index = plast - idx;
                if (_offset1 === 0) {
                    minInd = index;
                    maxInd = index;
                    curr = obj.fn(null, arr, index, 1);
                    target = index === plast ? curr : obj.fnP(arr[index + 1][AS], curr);
                    value = arr[index--][ordColumn];
                    while (index > pstart && value === arr[index][ordColumn]) {
                        target = obj.fnP(target, obj.fn(null, arr, index, 1));
                        minInd = index--;
                    }
                    while (minInd <= maxInd) arr[minInd++][AS] = target;
                    idx += maxInd - index;
                }
                else {
                    value = arr[index][ordColumn];
                    maxInd = minInd;
                    minInd = bsLeft(value != null ? value + offset1 : -Infinity, arr, pfirst, pend, gtfn) - 1;

                    if (minInd === plast) arr[index][AS] = nullValue;
                    else if (minInd < maxInd) {
                        target = obj.fn(null, arr, minInd + 1, maxInd - minInd);
                        arr[index][AS] = index === plast ? target : obj.fnP(arr[index + 1][AS], target);
                    }
                    else arr[index][AS] = arr[index + 1][AS];
                    idx++;
                }  
            }
        }
        pfirst = pend;
    }
    if (obj.avg) avgFn(arr, AS);
    if (obj.sum) sumFn(arr, AS, null);
};

const toGroups = (partitions, ptemp, temp, arr, fn, gnum, ordColumn) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, start, first, ptlen;

    while (++ind < plen) {
        partition = partitions[ind];
        ptlen = len(temp);
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            first = arr[index][ordColumn];
            start = index;
            index = pfirst + ++idx;
            while (idx < partition && arr[index][ordColumn] === first) index = pfirst + ++idx;
            fn(start, index - start);
            arr[start][gnum] = index - start;
            temp.push(arr[start]);
        }
        ptemp.push(len(temp) - ptlen);
        pfirst += partition;
    }
};

const fromGroups = (partitions, arr, fn, gnum, AS) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, first, numb;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            first = fn(index);
            numb = -1;
            while (++numb < arr[index][gnum]) arr[pfirst + idx++][AS] = first;
        }
        pfirst += partition;
    }
};

const avg_fn = fn => (_, xs, ind, n) => {
    const to = ind + n;
    let sum = 0;
    let count = 0;
    let item;

    while (ind < to) {
        item = fn(xs[ind++]);
        sum += item.sum;
        count += item.count;
    }
    return { sum, count };
};

const executeGroups = (partitions, obj, arr, AS, up, uf, offset1, offset2, ordColumn) => {
    const gprop = Symbol();
    const gnum = Symbol();
    const avg = obj.avg;
    let temp = [];
    let ptemp = [];
    let fn;

    if (avg) obj.fromWin = true;
    fn = (index, n) => arr[index][gprop] = obj.fn(null, arr, index, n);
    toGroups(partitions, ptemp, temp, arr, fn, gnum, ordColumn);
    if (obj.count) { obj = _SUM(gprop); obj.count = true; }
    else obj = obj.groupsFn(gprop);
    if (avg) obj.fn = avg_fn(aFn(gprop));
    executeRows(ptemp, obj, temp, AS, up, uf, offset1, offset2);
    fn = index => arr[index][AS];
    return fromGroups(partitions, arr, fn, gnum, AS);
};

const executeRowNumber = (partitions, arr, AS) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = -1;
        while (++idx < partition) arr[pfirst + idx][AS] = idx + 1;
        pfirst += partition;
    }
};

const executeRank = (partitions, arr, AS, ordColumn, dense) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, first, value, dense_idx;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = 0;
        dense_idx = 1;
        while (idx < partition) {
            index = pfirst + idx;
            first = arr[index][ordColumn];
            value = dense ? dense_idx++ : idx + 1;
            arr[index][AS] = value;
            index = pfirst + ++idx;
            while (idx < partition && arr[index][ordColumn] === first) {
                arr[index][AS] = value;
                index = pfirst + ++idx;
            }
        }
        pfirst += partition;
    }
};

const executePercentRank = (partitions, arr, AS, ordColumn) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, first, value;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            first = arr[index][ordColumn];
            value = idx ? idx / (partition - 1) : 0;
            arr[index][AS] = value;
            index = pfirst + ++idx;
            while (idx < partition && arr[index][ordColumn] === first) {
                arr[index][AS] = value;
                index = pfirst + ++idx;
            }
        }
        pfirst += partition;
    }
};

const executeCumeDist = (partitions, arr, AS, ordColumn) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, first, value, start, ind_num, num;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = 0;
        while (idx < partition) {
            index = pfirst + idx;
            first = arr[index][ordColumn];
            start = idx;
            index = pfirst + ++idx;
            while (idx < partition && arr[index][ordColumn] === first) {
                index = pfirst + ++idx;
            }

            value = idx / partition;
            num = idx - start;
            idx = start;
            ind_num = -1;
            while (idx < partition && ++ind_num < num) {
                arr[pfirst + idx++][AS] = value;
            }
        }
        pfirst += partition;
    }
};


const executeNTile = (partitions, arr, AS, _, n) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, fix, rem, ind_tile, ind_fix;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = 0;
        fix = floor(partition / n);
        rem = partition % n;
        ind_tile = 0;
        while (ind_tile < n) {
            ind_fix = -1;
            while (++ind_fix < fix) {
                index = pfirst + idx;
                arr[index][AS] = ind_tile + 1;
                idx++;
            }
            if (rem) {
                rem--;
                index = pfirst + idx;
                arr[index][AS] = ind_tile + 1;
                idx++;
            }
            ind_tile++;
        }
        pfirst += partition;
    }
};

const executeLead = (partitions, arr, p, offset, def, AS) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, ind_offset;

    while (++ind < plen) {
        partition = partitions[ind];
        idx = -1;
        while (++idx < partition) {
            index = pfirst + idx;
            ind_offset = idx + offset;
            arr[index][AS] = ind_offset >= partition || ind_offset < 0  ? def : arr[index + offset][p];
        }
        pfirst += partition;
    }
};

const executeFirstValue = (partitions, arr, p, n, AS) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, value, offset;

    while (++ind < plen) {
        partition = partitions[ind];
        offset = n < 0 ? partition - 1 : n;
        value = offset >= partition ? null : arr[pfirst + offset][p];
        idx = -1;
        while (++idx < partition) {
            index = pfirst + idx;
            arr[index][AS] = value;
        }
        pfirst += partition;
    }
};


const checkIntFB = num => Number.isInteger(num) && num >= 0;
const checkFloatFB = num => isNumber(num) && num >= 0;

const err1 = 'invalid frame boundaries';
const more = 'more frame boundaries expected';
const less = 'less frame boundaries expected';
const gte = '(>= 0)';

const checkUnBounded = (fb, up, range, index) => {
    let ind = up ? 1 : 0;
    let next = fb[ind++];
    if (isNumber(next)) {
        const checkFn = range ? checkFloatFB : checkIntFB;
        if (!checkFn(next))
            _throw(`${ range ? 'number' : 'integer' } ${ gte } expected (SELECT ${ index })`);
        next = fb[ind];
        if (!(next && (next.prec || next.fol))) _throw(`${ err1 } (SELECT ${ index })`);
    }
    else if (!(next && (next.cr || (up && next.uf)))) _throw(`${ err1 } (SELECT ${ index })`);
};

const unBoundedFn = (fb, partitions, obj, arr, AS, fn, ordColumn) => {
    let up = !!fb[0].up;
    let ind = up ? 1 : 0;
    let next = fb[ind++];
    if (next.cr) next = 0;
    if (isNumber(next)) {
        let num = next;
        next = fb[ind];
        if(num > 0 && next.prec) num = -num;
        fn(partitions, obj, arr, AS, up, !up, up ? -Infinity : num, up ? num : Infinity, ordColumn);
    }
    else executeUnordered(partitions, obj, arr, AS);
};

const checkB = (fb, ind, range, index) => {
    let next = fb[ind++];
    if (isNumber(next)) {
        const checkFn = range ? checkFloatFB : checkIntFB;
        if (!checkFn(next))
            _throw(`${ range ? 'number' : 'integer' } ${ gte } expected (SELECT ${ index })`);
        next = fb[ind++];
        if (!(next && (next.prec || next.fol))) _throw(`${ err1 } (SELECT ${ index })`);
    }
    else if (next && next.cr) next = PRECEDING;
    else _throw(`${ err1 } (SELECT ${ index })`);
    return { next, ind };
};

const checkBounded = (fb, range, index) => {
    let { next: fs, ind } = checkB(fb, 0, range, index);
    let { next: fe, ind: idx } = checkB(fb, ind, range, index);
    if (len(fb) !== idx) _throw(`${ err1 } (SELECT ${ index })`);
    if (fs.fol && fe.prec) _throw(`${ err1 } (SELECT ${ index })`);
};

const bFn = (fb, ind) => {
    let next = fb[ind++];
    let num = 0;
    if (isNumber(next)) {
        num = next;
        next = fb[ind++];
        if(num > 0 && next.prec) num = -num;
    }
    return { num, ind };
};

const boundedFn =  (fb, partitions, obj, arr, AS, fn, ordColumn) => {
    let { num: num1, ind } = bFn(fb, 0);
    let { num: num2 } = bFn(fb, ind);
    if (num1 > num2) exNulls(obj, arr, AS);
    else fn(partitions, obj, arr, AS, true, true, num1, num2, ordColumn);
};

const isUnbounded = fb => (fb[0] != null && fb[0].up) || (last(fb) != null && last(fb).uf);

const checkFrame = (frame, range, index) => {
    const fb = frame['frame_boundaries'];
    const fb_len = len(fb);
    if (fb_len < 2) _throw(`${ more } (SELECT ${ index })`);
    if (fb_len > 4) _throw(`${ less } (SELECT ${ index })`);
    if (isUnbounded(fb)) checkUnBounded(fb, fb[0].up, range, index);
    else checkBounded(fb, range, index);
};

const frameFn = (frame, partitions, obj, arr, AS, ordColumn) => {
    const fb = frame['frame_boundaries'];
    const fn = isUnbounded(fb) ? unBoundedFn : boundedFn;
    fn(fb, partitions, obj, arr, AS, frame.fn, ordColumn);
};

const frameAbbr = (fn, ofs, fs) => {
    if (isNumber(ofs)) {
        if (fs && fs.prec) return fn(ofs, fs, CURRENT_ROW);
        return fn(CURRENT_ROW, ofs, fs);
    }
    if (ofs && ofs.up) return fn(ofs, CURRENT_ROW);
    return fn(CURRENT_ROW, ofs);
};

const checkSub = (sub, xs, ambs, from, clause) => {
    if (!isSubset(sub, xs)) {
        const diffs = difference(sub, xs);
        const ambiguous = intersect(diffs, ambs);
        const wrongColumn = len(ambiguous) ? ambiguous[0] : diffs[0];
        if (isObject(from)) from = `SELECT ${ from.ind }`;
        else if (isArray(from)) from = `SELECT ${ (map (fnToAlias) (from)).indexOf(wrongColumn) + 1 }`;
        if (len(ambiguous)) _throw(`column ${ wrongColumn } (${ from }) ambiguous${ clause ? clause : ''}`);
        _throw(`column ${ wrongColumn } (${ from }) does not exist${ clause ? clause : ''}`);
    }
};

const stringCheck = (str, from) => {
    if (!(isString(str) && str)) _throw(`string expected (${ from })`);
};

const strColsCheck = (columns, from) => {
    if (nul(columns)) _throw(`string expected (${ from })`);
    for (let col of columns) stringCheck(col, from);
};

const checkStr = (str, from) => {
    if (str === '*') _throw(`asterisk (${ from })`);
    if (isElem('.', str)) _throw(`${ str }: point character (${ from })`);
    if (parseInt(str) + '' === str) _throw(`${ str }: all digits (${ from })`);
    if (!col(str) || {}[str]) _throw(`${ str }: invalid name (${ from })`);
};

const strNulCheck = (str, from) => {
    if (nul(str)) {
        if (isArray(from)) from = `SELECT ${ from.indexOf(str) + 1 }`;
        _throw(`empty string (${ from })`);
    }
};

const checkColumnStr = (str, from) => {
    strNulCheck(str, from);
    if (trim(str) !== str) _throw(`${ trim(str) }: leading/trailing space (${ from })`);
    checkStr(str, from);
};

const pgFn = ind => !(ind === 0 || ind === 2 || ind === 4);

const checkOrder = (ordby, checkOrdIndFn, err_str) => {
    const res = [];
    const orderby_columns = ordby['columns'];
    const oc_len = len(orderby_columns);
    let ind = 0;
    let obj_ok = false;
    let column, ord_ind;

    while (ind < oc_len) {
        column = orderby_columns[ind];
        if (obj_ok) {
            if (isObject(column)) {
                ord_ind = column['ord_ind'];
                if (checkOrdIndFn(ord_ind)) _throw(`invalid option in ${ err_str }`);
                obj_ok = false;
            }
            else res.push(column);
        }
        else {
            if (isString(column)) { res.push(column); obj_ok = true; }
            else _throw(`invalid parameters (${ err_str })`);
        }
        ind++;
    }
    return res;
};

const order_fns = [
    col => ascn(obj => obj[col]),
    col => descn(obj => obj[col]),
    col => ascn(obj => toLower(obj[col])),
    col => descn(obj => toLower(obj[col])),
    col => ascn_loc(obj => obj[col]),
    col => descn_loc(obj => obj[col]),
];

const orderbyFn = (orderby, res) => {
    if (res && nul(res)) return [];
    const orderby_columns = orderby['columns'];
    const cmp_fns = [];
    const oc_len = len(orderby_columns);
    let ind = 0;
    let column, next, str_last;

    while (ind < oc_len) {
        column = orderby_columns[ind];
        str_last = true;
        if (ind + 1 < oc_len) {
            next = orderby_columns[ind + 1];
            if (isObject(next)) {
                cmp_fns.push(order_fns[next['ord_ind']](column));
                str_last = false;
                ind++;
            }
            else cmp_fns.push(order_fns[0](column));
        }
        ind++;
    }
    if (ind === oc_len && str_last) cmp_fns.push(order_fns[0](column));
    return res ? res.sort(cmp(cmp_fns)) : cmp_fns;
};

const partitionFn = (partitionby, orderby, res) => {
    let part_cmp = orderbyFn(partitionby);
    const _eq = _eqs(map (fn => (x, y) => fn(x, y) === 0) (part_cmp));
    if (orderby) part_cmp = append (part_cmp, orderbyFn(orderby));
    const _cmp = cmp(part_cmp);
    res.sort(_cmp);
    return _section(_eq, res);
};

const checkOver = (fn, input, ambs, index) => {
    const over = fn['WIN'];
    const clauses = over['clauses'];
    const framed = fn.framed;
    let last_ind = -1;
    let ord, partitionby, orderby, frame, partColumns, ordColumns;

    for (let clause of clauses) {
        ord = clause.over_ind;
        if (ord < last_ind) _throw(`OVER clauses: wrong order (SELECT ${ index })`);
        if (ord === last_ind) _throw(`OVER clauses: duplicates (SELECT ${ index })`);
        if (ord === 0) partitionby = clause;
        else if (ord === 1) orderby = clause;
        else if (ord === 2) frame = clause;
        else _throw(`OVER clauses: invalid clause (SELECT ${ index })`);
        last_ind = ord;
    }

    if (partitionby) {
        const err_str = `PARTITION_BY ${ index }`;
        partColumns = checkOrder(partitionby, pgFn, err_str);
        strColsCheck(partColumns, err_str);
        checkSub(partColumns, input, ambs, err_str);
    }

    if (orderby) ordColumns = checkOrderBy(orderby, input, ambs, index, fn.med);

    if (framed) {
        if (orderby) {
            if (!frame) frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW);
            if (frame.range && len(ordColumns) !== 1)
                _throw(`1 column expected (ORDER_BY ${ index })`);
        }
        else {
            if (!frame) frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING);
            if (frame.range) _throw(`ORDER_BY expected (SELECT ${ index })`);
        }

        checkFrame(frame, frame.range, fn.ind);
    }
    else {
        if (orderby) {
            if (!(fn.row_num || len(ordColumns) === 1))
                _throw(`1 column expected (ORDER_BY ${ fn.wg ? 'WITHIN_GROUP ' : '' }${ index })`);
        }
        else {
            if (!fn.row_num)
                _throw(`ORDER_BY expected (${ fn.wg ? 'WITHIN_GROUP ' : 'SELECT ' }${ index })`);
        }
    }

    over.props = { partitionby, orderby, frame, partColumns, ordColumns, framed };
};

const overFn = (over, arr, aggr_obj, AS) => {
    const { partitionby, orderby, frame, ordColumns, framed } = over.props;
    const partitions = partitionby ? partitionFn(partitionby, orderby, arr) :
                       orderby ? (orderbyFn(orderby, arr), [len(arr)]) : [len(arr)];

    if (orderby) {
        if (framed) {
            if (frame.range) {
                const last_oc = last(orderby['columns']);
                if (isObject(last_oc) &&
                   (last_oc.ord_ind === 1 || last_oc.ord_ind === 3 || last_oc.ord_ind === 5)) aggr_obj.desc = true;
            }
            frameFn(frame, partitions, aggr_obj, arr, AS, last(ordColumns));
        }
        else aggr_obj.fn(partitions, arr, last(ordColumns));
    }
    else {
        if (framed) executeUnordered(partitions, aggr_obj, arr, AS);
        else aggr_obj.fn(partitions, arr);
    }
};

const checkProp = p => {
    if (isString(p)) p = trim(p);
    else if (isObject(p)) p['nested'] = true;
    return p;
};

const aggrWin = (WIN, AS, err, expr, prop) =>
    ({ aggregate: true, win: true, WIN, AS, isFn: true, winonly: true, err, expr, prop });

const _RN = (WIN, AS, fn, dense_n, win) => win ? 
    { fn (arr) { overFn(WIN, arr, _RN(null, this.AS['alias_str'], fn, dense_n)); }, AS } :
    { fn (obj, arr, ordColumn) { fn(obj, arr, this.AS, ordColumn, dense_n); }, dense_n, AS };


const gte_zero = num => Number.isInteger(num) && num >= 0;
const gt_zero = num => Number.isInteger(num) && num > 0;
const _AS = ps => last(ps) && last(ps).alias ? last(ps) : null;
const _err = (num, plen, err) => plen < num ? 'more args expected' : plen > num ? 'too many args' : err;
const _WIN = ps => {
    const index = ps.findIndex(p => p && p.OVER);
    const win = index > -1;
    const WIN = win ? ps[index] : null;
    return { win, WIN };
}

const RN = (ps, fn, dense) => {
    let num = 1;
    const AS = _AS(ps);
    if (AS) num++;
    const { WIN } = _WIN(ps);
    let err;
    if (fn === executeNTile) {
        dense = ps[0];
        if (!gt_zero(dense)) err = 'integer (> 0) expected';
        num++;
    }
    const win = aggrWin(WIN, AS, _err(num, len(ps), err));
    const obj = _RN(WIN, AS, fn, dense, true);
    return extend(win, obj);
};

const checkOffset = (offset_WIN, def_WIN_AS, WIN_AS, num) => {
    let offset = 1;
    let def = null;
    let WIN = WIN_AS;
    let err;
    if (isObject(offset_WIN) && offset_WIN.OVER) WIN = offset_WIN;
    else {
        if (gte_zero(offset_WIN)) offset = offset_WIN;
        else err = 'integer (>= 0) expected';
        num++;
        if (isObject(def_WIN_AS) && def_WIN_AS.OVER) WIN = def_WIN_AS;
        else { def = def_WIN_AS; num++; };
    }
    return { offset, def, WIN, err, num };
};

const _LEAD_LAG = (prop, offset, def, WIN, AS, win) => win ? 
    { fn (arr) { overFn(WIN, arr, _LEAD_LAG(prop, offset, def, null, this.AS['alias_str'])); }, prop, AS } :
    { fn (obj_arr, arr) { executeLead(obj_arr, arr, prop, offset, def, this.AS); }, prop, AS };

const LEAD_LAG = (ps, mod) => {
    const p = checkProp(ps[0]);
    const AS = _AS(ps);
    const { offset, def, WIN, err, num } = checkOffset(ps[1], ps[2], ps[3], AS ? 3 : 2);
    const aggr = aggrWin(WIN, AS, _err(num, len(ps), err), true, p);
    const obj = _LEAD_LAG(p, mod * offset, def, WIN, AS, true);
    return extend (obj, aggr);
};

const _VALUE = (prop, WIN, AS, n, win) => win ? 
    { fn (arr) { overFn(WIN, arr, _VALUE(this.prop, null, this.AS['alias_str'], n)); }, prop, AS } :
    { fn (obj, arr) { executeFirstValue(obj, arr, this.prop, n, AS); }, prop, AS };


const VALUE = (ps, n) => {
    let num = 2;
    const p = checkProp(ps[0]);
    const AS = _AS(ps);
    if (AS) num++;
    const { WIN } = _WIN(ps);
    let err;
    if (n > 0) {
        n = ps[1];
        if (!gt_zero(n)) err = 'integer (> 0) expected';
        n--; num++;
    }
    const win = aggrWin(WIN, AS, _err(num, len(ps), err), true, p);
    const obj = _VALUE(p, WIN, AS, n, true);
    return extend (obj, win);
};

const percentileCont = (arr, p, cont, num) => {
    const n = len(arr);
    if (n === 0) return null;
    const ind = 0;
    if (!cont) return arr[ind + floor(1 + num * (n - 1)) - 1][p];
    const RN = 1 + num * (n - 1);
    const FRN = floor(RN);
    const CRN = ceil(RN);
    const floor_value = arr[ind + FRN - 1][p];
    const ceiling_value = arr[ind + CRN -1][p];
    return floor_value + (ceiling_value - floor_value) * (RN - FRN);
};

const filterRange = (start, n, p, arr) => {
    const res = [];
    const to = start + n;
    let ind = start;

    while (ind < to) {
        if (arr[ind][p] != null) res.push(arr[ind]);
        ind++;
    }
    return res;
};

const exPWin = (partitions, arr, AS, p, num, cont) => {
    const plen = len(partitions);
    let pfirst = 0;
    let ind = -1;
    let partition, idx, index, value, _arr;

    while (++ind < plen) {
        partition = partitions[ind];
        _arr = filterRange(pfirst, partition, p, arr);
        value = percentileCont(_arr, p, cont, num);
        idx = -1;
        while (++idx < partition) {
            index = pfirst + idx;
            arr[index][AS] = value;
        }
        pfirst += partition;
    }
};

const _PERCENTILE = (prop, num, AS_WIN, AS, cont, win) => {
    return win ? {
        fn (arr) { overFn(AS_WIN, arr, _PERCENTILE(this.prop, num, null, this.AS['alias_str'], cont)); },
        prop,
        AS
    } :
    {
        fn (obj_arr, arr, start, n) {
            if (isArray(obj_arr)) { exPWin(obj_arr, arr, AS, this.prop, num, cont); return; }
            arr = orderbyFn(this.orderby, filterRange(start, n, this.prop, arr));
            const res = percentileCont(arr, this.prop, cont, num);
            return this.AS ? obj_arr[this.AS['alias_str']] = res : res;
        },
        prop,
        AS: AS_WIN
    };
};

const PERCENTILE = (ps, cont, err, med) => {
    let num = 2;
    const AS = _AS(ps);
    if (AS) num++;
    const { win, WIN } = _WIN(ps);
    if (win) num++;
    const ind = ps.findIndex(p => p && p.wg);
    const WG = ind > -1 ? ps[ind] : null;
    let n = ps[0];
    if (!(isNumber(n) && n >= 0 && n <= 1)) err = 'number (0 <= n <= 1) expected';
    return { aggregate: true, win, aggr: !win, WIN, wg: true, WG, AS, med,
        framed: false, expr: false, isFn: true, err: _err(num, len(ps), err) , params: { n, cont } };
};

const median = ps => {
    let p = ps[0];
    const arr = [0.5, WITHIN_GROUP(ORDER_BY(isString(p) ? trim(p) : p))];
    let num = 1;
    const { win, WIN } = _WIN(ps);
    if (win) { arr.push(WIN), num++;}
    const AS = _AS(ps);
    if (AS) { arr.push(AS); num++; }
    return PERCENTILE(arr, true, _err(num, len(ps), null), true);
};

const aggr = (ps, fn) => {
    let num = 1;
    const AS = _AS(ps);
    if (AS) num++;
    const { win, WIN } = _WIN(ps);
    if (win) num++;
    const err = _err(num, len(ps), null);
    const prop = checkProp(ps[0]);
    const obj = { aggregate: true, win, aggr: !win, WIN, prop, AS, framed: true, expr: true, isFn: true, err };
    const objfn = fn(prop, win ? WIN : AS, AS, win);
    return Object.assign(obj, objfn);
};

const uniq = (xs, fn, ind, n) => {
    const res = [];
    const set = new Set();
    const to = ind + n;
    let item;

    while (ind < to) {
        item = xs[ind++];
        if (!set.has(fn(item))) { set.add(fn(item)); res.push(item); }
    }
    return res;
};

const aggrDist = (ps, fn) => {
    let num = 1;
    const AS = _AS(ps);
    if (AS) num++;
    const err = _err(num, len(ps), null);
    const prop = checkProp(ps[0]);
    return { aggregate: true, win: false, aggr: true, prop, AS, expr: true, isFn: true, err,
        fn (obj, arr, start, n) {
            arr = uniq(arr, aFn(prop), start, n);
            return fn(prop, AS)['fn'](obj, arr, 0, len(arr));
        }
    };
};

const svc = ':';
const _take = str => _slice(str, 1, - 1);
const col = p => !(p[0] === svc && p[len(p) - 1] === svc);

const scalarFn = scalar => obj => {
    const ps = scalar['nested'] ? scalar['props'] : init(scalar['props']);
    const inputs = map (p => (isString(p) && col(p)) || isSymbol(p) ? obj[p] :
                             isNumber(p) ? p :
                             isString(p) ? _take(p) :
                             scalarFn(p)(obj)) (ps);
    const res = scalar['fn'](...inputs);
    return scalar['nested'] ? res : obj[last(scalar['props'])['alias_str']] = res;
};

const isSymbol = x => typeof x === 'symbol';
const aFn = p => isString(p) || isSymbol(p)  ? obj => obj[p] : scalarFn(p);

const _count = (xs, fn, ind, n) => {
    if (n === 1) return fn(xs[ind]) == null ? 0 : 1;
    const to = ind + n;
    let count = 0;

    while (ind < to) if (fn(xs[ind++]) != null) count++;
    return count;
};

const _COUNT = (prop, AS_WIN, AS, win) => win ? 
    { fn (arr) { overFn(AS_WIN, arr, _COUNT(this.prop), this.AS['alias_str']); }, prop, AS, count: true } :
    { 
        fn (obj, arr, start, n) {
            const res = this.prop === '*' ? n : _count(arr, aFn(this.prop), start, n);
            return this.AS ? obj[this.AS['alias_str']] = res : res;
        },
        fnP (x, y) { return x + y; },
        fnM (x, y) { return x - y; },
        count: true,
        prop,
        AS: AS_WIN
    };


const objP = (x, y) => ({ sum: (x ? x.sum : 0) + (y ? y.sum : 0), count: (x ? x.count : 0) + (y ? y.count : 0) });
const objM = (x, y) => ({ sum: (x ? x.sum : 0) - (y ? y.sum : 0), count: (x ? x.count : 0) - (y ? y.count : 0) });

const _sum = (xs, fn, ind, n, win) => {
    const to = ind + n;
    let sum = 0;
    let count = 0;
    let item;

    while (ind < to) {
        item = fn(xs[ind++]);
        if (item != null) { sum += item; count++; }
    }
    return win ? { sum, count } : count ? sum : null;
};

const _SUM = (prop, AS_WIN, AS, win) => {
    return win ? { fn (arr) { overFn(AS_WIN, arr, _SUM(this.prop), this.AS['alias_str']); }, prop, AS } :
    {
        fn (obj, arr, start, n) {
            const res = _sum(arr, aFn(this.prop), start, n, this && this.fromWin);
            return this.AS ? obj[this.AS['alias_str']] = res : res;
        },
        fnP (x, y)  { return objP(x, y); },
        fnM (x, y)  { return objM(x, y); },
        sum: true,
        prop,
        AS: AS_WIN,
        groupsFn: _SUM
    }
};

const _avg = (xs, fn, ind, n, win) => {
    const to = ind + n;
    let sum = 0;
    let count = 0;
    let item;

    while (ind < to) {
        item = fn(xs[ind++]);
        if (item != null) { sum += item; count++; }
    }
    return win ? { sum, count } : count ? sum / count : null;
};

const _AVG = (prop, AS_WIN, AS, win) => {
    return win ? { fn (arr) { overFn(AS_WIN, arr, _AVG(this.prop), this.AS['alias_str']); }, prop, AS } :
    {
        fn (obj, arr, start, n) {
            const res = _avg(arr, aFn(this.prop), start, n, this && this.fromWin);
            return this.AS ? obj[this.AS['alias_str']] = res : res;
        },
        fnP (x, y) { return objP(x, y); },
        fnM (x, y) { return objM(x, y); },
        avg: true,
        prop,
        AS: AS_WIN,
        groupsFn: _AVG
    }
};

const _min = (x, y) => x != null && y != null ? (y < x ? y : x) : x == null ? y : x;
const _max = (x, y) => x != null && y != null ? (y > x ? y : x) : x == null ? y : x; 

const minmax = (xs, fn, ind, n, fnn) => {
    if (n === 1) return fn(xs[ind]);
    const to = ind + n;
    let res = fn(xs[ind++]);

    while (ind < to) res = fnn(fn(xs[ind++]), res);
    return res;
};

const treeIns = (xs, fn, ind, n, tree) => {
    const to = ind + n;
    let item;

    while (ind < to) {
        item = fn(xs[ind]);
        if (item != null) tree.insert(item);
        ind++;
    }
};

const treeRem = (xs, fn, ind, n, tree) => {
    const to = ind + n;
    let item;

    while (ind < to) {
        item = fn(xs[ind]);
        if (item != null) tree.remove(item);
        ind++;
    }
};

const _MIN = (prop, AS_WIN, AS, win) => {
    return win ? { fn (arr) { overFn(AS_WIN, arr, _MIN(this.prop), this.AS['alias_str']); }, prop, AS } :
    {
        fn (obj, arr, start, n, tree, remove) {
            const fn = tree ? (remove ? treeRem : treeIns) : minmax;
            const res = fn(arr, aFn(this.prop), start, n, tree || _min);
            return this.AS ? obj[this.AS['alias_str']] = res : res;
        },
        fnP : _min,
        min: true,
        prop,
        AS: AS_WIN,
        groupsFn: _MIN
    };
};


const _MAX = (prop, AS_WIN, AS, win) => {
    return win ? { fn (arr) { overFn(AS_WIN, arr, _MAX(this.prop), this.AS['alias_str']); }, prop, AS } :
    {
        fn (obj, arr, start, n, tree, remove) {
            const fn = tree ? (remove ? treeRem : treeIns) : minmax;
            const res = fn(arr, aFn(this.prop), start, n, tree || _max);
            return this.AS ? obj[this.AS['alias_str']] = res : res;
        },
        fnP: _max,
        max: true,
        prop,
        AS: AS_WIN,
        groupsFn: _MAX
    };
};

const _section = (eq, xs) => {
    const xlen = len(xs);
    const res = [];
    let idx = 0;
    let ind = 0;
    let index;

    while (ind < xlen) {
        index = ind + 1;
        while (index < xlen && eq(xs[ind], xs[index])) index++;
        res[idx++] = index - ind;
        ind = index;
    }
    return res;
};

const _math = (xs, fn) => {
    const xlen = len(xs);
    if (!xlen) return 0;
    let ind = 0;
    let res = xs[ind++];
    if (res == null) return null;
    let item;

    while (ind < xlen) {
        item = xs[ind++];
        if (item == null) return null;
        else res = fn(res, item);
    }
    return res;
};

const add = (...xs) => _math(xs, (x, y) => x + y);
const sub = (...xs) => _math(xs, (x, y) => x - y);
const mul = (...xs) => _math(xs, (x, y) => x * y);
const div = (...xs) => _math(xs, (x, y) => x / y);

const scalar = (fn, ps) => {
    ps = map (checkProp) (ps);
    return { scalar: true, fn, props: ps, isFn: true };
};

const _len = ps => ps[0] == null ? null : ps[0].length;
const _round = ps => ps[0] == null ? null : Math.round(ps[0]);

const checkAliasString = (alias, ind) => {
    if (isString(alias)) alias = trim(alias);
    stringCheck(alias, `SELECT ${ ind } AS`);
    checkStr(alias, `SELECT ${ ind } AS`);
    return alias;
};

const checkScalarAliases = fn => {
    const props = fn.props;
    if (!nul(props) && last(props)['alias']) {
        let alias = last(props)['alias_str'];
        alias = checkAliasString(alias, fn.ind);
        last(props)['alias_str'] = alias;
        return alias;
    }
    else _throw(`AS expected (SELECT ${ fn.ind })`);
};

const checkAggregateAliases = fn => {
    if (fn['AS'] && fn['AS']['alias']) {
        let alias = fn['AS']['alias_str'];
        alias = checkAliasString(alias, fn.ind);
        fn['AS']['alias_str'] = alias;
        return alias;
    }
    else _throw(`AS expected (SELECT ${ fn.ind })`);
};

const _isAggr = ps => any(p => isObject(p) && (p.aggr || (p.scalar && _isAggr(p.props)))) (ps);

const isAggr = (ps, aa) => {
    return any(p => (isString(p) && isElem(p, aa)) ||
                    (isObject(p) && (p.aggr || (p.scalar && isAggr(p.props, aa))))) (ps);
};

const isWin = (ps, wa) => {
    return any(p => (isString(p) && isElem(p, wa)) ||
                    (isObject(p) && (p.win || (p.scalar && isWin(p.props, wa))))) (ps);
};

const checkScalar = (fn, aa, wa, alias) => {
    const ps = init(fn.props);

    if (isWin(ps, wa)) { fn.win = true; wa.push(alias); if (_isAggr(ps)) fn.winaggr = true; return; }
    if (isAggr(ps, aa)) { fn.aggr = true; aa.push(alias); return; }
    fn.scal = true;
};

const checkAliases = fns => {
    const aa = [];
    const wa = [];
    let alias;

    for (let fn of fns) {
        if (fn.scalar) {
            alias = checkScalarAliases(fn);
            checkScalar(fn, aa, wa, alias);
        }
        else {
            alias = checkAggregateAliases(fn);
            if (fn.win) {
                wa.push(alias);
                const prop = fn.prop;
                if ((isObject(prop) && (prop.aggr || (prop.scalar && _isAggr(prop.props))))) fn.winaggr = true;
            }
            else aa.push(alias);
        }
    }
    return { aa, wa };
};

const _checkProp = (p, ind) => {
    if (isString(p) ? nul(p) : !(isObject(p) && p.isFn)) {
        _throw(`invalid parameter (SELECT ${ ind })`);
    }
};

const checkParams = (ps, ind) => {
    if (nul(ps)) _throw(`parameters expected (SELECT ${ ind })`);
    for (let p of ps) {
        if (isString(p) ? nul(p) : isObject(p) ? !p.isFn : !isNumber(p)) {
            if (isObject(p) && p.alias) _throw(`nested AS (SELECT ${ ind })`);
            _throw(`invalid parameter (SELECT ${ ind })`);
        }
    }
};

const checkWG = fn => {
    if (!(fn.WG && fn.WG.wg_ind === 1)) _throw(`WITHIN_GROUP expected (SELECT ${ fn.ind })`);
    if (!(fn.WG.orderby && fn.WG.orderby.wg_ind === 0))
        _throw(`ORDER_BY expected (WITHIN_GROUP SELECT ${ fn.ind })`);
    if (fn.aggr) fn.orderby = fn.WG.orderby;
    fn.WIN = OVER(...(fn.win ? fn.WIN.clauses : []), fn.WG.orderby);
};

const appendObj = fn => {
    fn.prop = fn.WIN.props.ordColumns[0];
    const sum_obj = _PERCENTILE(fn.prop, fn.params.n, fn.win ? fn.WIN : fn.AS, fn.AS, fn.params.cont, fn.win);
    Object.assign(fn, sum_obj);
};

const checkAggregateFn = (fn, input, ambs, ind) => {
    if (fn.nested && fn.AS) _throw(`nested AS (SELECT ${ ind })`);
    if (fn.winonly && !(fn.WIN && fn.WIN.OVER)) _throw(`OVER expected (SELECT ${ ind })`);
    if (fn.wg) checkWG(fn);
    if (fn.err) _throw(`${ fn.err } (SELECT ${ ind })`);
    if (fn.expr) _checkProp(fn.prop, ind);
    if (fn.win || fn.wg) checkOver(fn, input, ambs, ind);
    if (fn.wg) appendObj(fn);
};

const isNested = (aggr, p, aliases) => isElem(p, aliases.wa) || (aggr && isElem(p, aliases.aa));

const scalInd = 0, startInd = 1, restInd = 2, overInd = 3;
const checkNesting = (fn, root, input, ambs, ind, aliases) => {
    const res = [[],[],[],[]];
    if (fn.aggregate) {
        if (root === scalInd || (fn.win && root === restInd)) _throw(`nesting error (SELECT ${ ind })`);
        root = fn.aggr ? scalInd : restInd;
        checkAggregateFn(fn, input, ambs, ind);
        const prop = fn.prop;
        if (fn.win) {
            const partCols = fn.WIN.props.partColumns;
            const ordCols = fn.WIN.props.ordColumns;
            if (partCols) res[overInd].push(...partCols);
            if (ordCols) res[overInd].push(...ordCols);
        }
        if (isString(prop) && !(fn.count && prop === '*')) {
            if (isNested(fn.aggr, prop, aliases)) _throw(`nesting error (SELECT ${ ind + ' ' + prop })`);
            res[root].push(prop);
        }
        else if (isObject(prop)) {
            const names = checkNesting(prop, root, input, ambs, ind, aliases);
            each ((n, ind) => res[ind].push(...n)) (names);
        }
    }
    else {
        const ps = fn.nested ? fn.props : init(fn.props);
        checkParams(ps, ind);
        for (let p of ps) {
            if ((isString(p) && col(p))) {
                if (aliases && root !== startInd && isNested(root === scalInd, p, aliases))
                    _throw(`nesting error (SELECT ${ ind + ' ' + p })`);
                res[root].push(p);
            }
            else if (isObject(p)) {
                const names = checkNesting(p, root, input, ambs, ind, aliases);
                each ((n, ind) => res[ind].push(...n)) (names);
            }
        }
    }
    return res;
};

const aliasFn = fn => (fn['aggregate'] ? fn['AS'] : last(fn['props']))['alias_str'];

const checkScal = (fns, input, ambs) => {
    const scal_names = [];
    const sa = [];
    let alias, names;

    for (let fn of fns) {
        names = checkNesting(fn, scalInd, null, ambs, fn.ind, null);
        checkSub(names[scalInd], input, ambs, fn);
        alias = aliasFn(fn); input.push(alias); sa.push(alias);
        scal_names.push(...names[0]);
    }
    return { scal_names, sa };
};

const checkAggr = (fns, sa_input, ambs, gc, aliases) => {
    const aggr_names = [];
    const aa = [];
    let names;

    for (let fn of fns) {
        names = checkNesting(fn, startInd, sa_input, ambs, fn.ind, aliases);
        checkSub(names[scalInd], sa_input, ambs, fn);
        checkSub(names[startInd], append(aa, gc), [], fn);
        aa.push(aliasFn(fn)); 
        aggr_names.push(...names[0], ...names[1]);
    }
    return { aggr_names, aa, input: append(gc, aa) };
};

const checkWin = (fns, input, sa_input, ambs, grouping, aliases) => {
    const win_names = [];
    const wa = [];
    let names;

    for (let fn of fns) {
        names = checkNesting(fn, startInd, input, ambs, fn.ind, aliases);
        checkSub(names[scalInd], sa_input, ambs, fn);
        checkSub(names[startInd], append(input, wa), grouping ? [] : ambs, fn);
        checkSub(names[restInd], input, grouping ? [] : ambs, fn);
        wa.push(aliasFn(fn));
        win_names.push(...names[0], ...names[1], ...names[2], ...names[3]);
    }
    input.push(...wa);
    return { win_names, wa };
};

const checkOrderBy = (orderby, input, ambs, index, med) => {
    const err_str = `${ med ? 'SELECT' : 'ORDER_BY' }${ index ? ' ' + index : '' }`;
    const columns = checkOrder(orderby, ind => ind < 0 || ind > 5, err_str);
    strColsCheck(columns, err_str);
    checkSub(columns, input, ambs, err_str);
    return columns;
};

const executeScalars = (table, scalars) => {
    const scalarFns = map (scalarFn) (scalars);
    for (let obj of table) {
        for (let scFn of scalarFns) scFn(obj);
    }
};

const whereFn = (where, table) => filter(where.pred)(table);

const groupbyFn = (groupby, res) => {
    if (nul(res)) return [];
    return partitionFn(groupby, null, res);
};

const scalarAggrFn = (sa, obj, arr, start, n) => {
    const ps = sa['nested'] ? sa['props'] : init(sa['props']);
    const inputs = map (p => isString(p) && col(p) ? obj[p] :
                             isNumber(p) ? p :
                             isString(p) ? _take(p) :
                             p['aggregate'] ? p['fn'](obj, arr, start, n) :
                             scalarAggrFn(p, obj, arr, start, n)) (ps);
    const res = sa['fn'](...inputs);
    return sa['nested'] ? res : obj[last(sa['props'])['alias_str']] = res;
};

const _scalarAggrFn = sa => (obj, arr, start, n) => scalarAggrFn(sa, obj, arr, start, n);

const waFn = col => {
    const obj = col.prop;
    const alias_str = Symbol();
    const alias = { alias_str };
    col.prop = alias_str;
    return obj.aggr ? (obj.AS = alias, obj.fn.bind(obj)) :
                      (obj.props.push(alias), obj.nested = false, _scalarAggrFn(obj));
};

const scalWaFn = sa => {
    const fns = map((p, ind, xs) => {
        if (isObject(p) && p.isFn) {
            if (p.win && !isString(p.prop)) return waFn(p);
            if (p.aggr) {
                const obj = p;
                const alias_str = Symbol();
                const alias = { alias_str };
                xs[ind] = alias_str;
                obj.AS = alias;
                return obj.fn.bind(obj);
            }
            if (p.scalar) return scalWaFn(p);
            return p;
        }
        return p;
    })(sa['props']);
    return (obj, arr, start, n) => {
        each (fn => isFunction(fn) ? fn(obj, arr, start, n) : fn) (fns);
    };
};

const executeAggregates = (partitions, arr, columns) => {
    const plen = len(partitions);
    const res = Array(plen);
    let ind = -1;
    while (++ind < plen) res[ind] = {};

    const fn = col => {
        const fnn = isString(col) ? (obj, arr, start) => obj[col] = arr[start][col] :
                                    col.aggregate ? col.aggr ? col.fn.bind(col) : waFn(col) :
                                                    col.aggr ? _scalarAggrFn(col) : scalWaFn(col);
        let pfirst = 0;
        let ind = -1;
        let partition;
    
        while (++ind < plen) {
            partition = partitions[ind];
            fnn(res[ind], arr, pfirst, partition);
            pfirst += partition;
        }
    };
    for (let col of columns) fn(col);
    return res;
};

const scalarWinFn = (fn, arr) => {
    fn.props = map (p => {
        if (p.aggregate && p.win) {
            const alias_str = Symbol();
            p.AS = { alias_str };
            p.fn(arr);
            p = alias_str;
        }
        else if (p.scalar) scalarWinFn(p, arr);
        return p;
    }) (fn['props']);
};

const executeWins = (res, wins) => {
    const scalars = [];
    for (let win of wins) {
        if (win.aggregate) win.fn(res);
        else {
            scalarWinFn(win, res);
            scalars.push(win);
        }
    }
    if (!nul(scalars)) executeScalars(res, scalars);
};

const fnToAlias = fn => isString(fn) ? fn : aliasFn(fn);

const getDistinct = (columns, res) => {
    const _cmp = cmp(map(col => ascn(obj => obj[col])) (columns));
    const _eq = _eqs(map(col => equalBy(obj => obj[col])) (columns));
    res.sort(_cmp);
    return nubSBy(_eq, res);
}

const checkStr1 = (tmp, str, tname) => {
    if (str && tmp[str] && !isString(tmp[str]))
        _throw(`Column ${ str } (FROM ${ tname }) ambiguous!`);
};

const mergeObjs = (obj1, obj2, _pr, amb, empty, using, str, tname) => {
    const pr = obj1[_pr];
    const tmp = obj1[amb];
    const res = {};

    if (using) {
        pr[obj1[str]] = str; if (!tmp[str]) tmp[str] = obj1[str];
        pr[obj2[str]] = str; delete obj2[str];
    }
    else { for (let key of keys(tmp)) tmp[key] = 1; }
    checkStr1(tmp, str, tname);

    let value;
    for (let key of keys(obj1)) {
        value = obj1[key];
        if (obj2[key] && !pr[value]) { res[value] = key; tmp[key] = value; }
        else res[key] = value;
    }

    for (let key of keys(obj2)) {
        value = obj2[key];
        if (tmp[key]) res[value] = key;
        else res[key] = value;
    }

    res[_pr] = pr;
    res[amb] = tmp;
    res[empty] = obj1[empty];
    return res;
};

const checkCrossJoin = (t1, t2, pr, amb, empty) => t1[empty] || t2[empty] ? (t1[empty] = true, t1) :
                                                   mergeObjs(t1, t2, pr, amb, empty);

const checkJoin = (t1, t2, str1, str2, join, ta1, ta2, using, pr, amb, empty) => {
    if (t1[empty] || t2[empty]) {
        if (join.anti) {
            if (join.left) { if (t2[empty]) return t1; }
            else { if (t1[empty]) return (t2[pr] = {}, t2[amb] = {}, t2); }
        }
        return (t1[empty] = true, t1);
    }
    const pre = using ? 'USING ' : 'ON ';
    stringCheck(str1, `FROM ${ pre + ta1 }`);
    stringCheck(str2, `FROM ${ pre + ta2 }`);
    if (!(t1[str1] || t1[pr][str1])) checkSub([str1], keys(t1), keys(t1[amb]), `FROM ${ pre + ta1 }`);
    if (!t2[str2]) checkSub([str2], keys(t2), [], `FROM ${ pre + ta2 }`);
    if (join.semi) {
        checkStr1(t1[amb], str1, ta1);
        return join.left ? t1 : (t2[pr] = {}, t2[amb] = {}, t2);
    }
    return mergeObjs(t1, t2, pr, amb, empty, using, str1, ta1);
};

const checkTNUniqness = names => {
    if (!isUniq(names)) {
        const set = new Set();
        let duplicate;
        let ind = 0;

        for (let name of names) {
            if (set.has(name)) { duplicate = name; break; }
            set.add(name);
            ind++;
        }
        _throw(`table name ${ duplicate } (FROM ${ 'table ' + ind + 1 }) already exists`);
    }
};


const checkNext = (tables, table, tas, ind, tlen) => {
    const next_ind = ind + 1;
    let name = 't' + (len(tas) + 1);
    let next;

    if (next_ind < tlen) {
        next = tables[next_ind];
        if (isString(next)) {
            name = trim(next);
            strNulCheck(name, 'FROM table-name');
            ind++;
        }
    }
    tas.push(name);

    checkTNUniqness(tas);
    if (!nul(table)) {
        for (let key of keys(table[0])) checkColumnStr(key, name + ' column');
    }
    return ind;
};

const copyObj = obj => Object.assign({}, obj);

const _throwErr = (join, name, lastItem) => {
    const fromStr = 'FROM: ';
    const afterStr = `after ${ name } `;

    if (join) {
        if (isArray(lastItem)) _throw(fromStr + afterStr + `ON/USING expected`);
        _throw(fromStr + `after JOIN ` + afterStr + `table expected`);
    }
    _throw(fromStr + (isArray(lastItem) ? `` : `after ON/USING `) + afterStr + `table or JOIN expected`);
};

const checkJoinTables = tables => {
    const tlen = len(tables);
    const tas = [];
    const inputs = [];
    const temps = [];
    const pr = Symbol();
    const amb = Symbol();
    const empty = Symbol();
    const fn = (ta, item) => mapKeys (k => ta + k, keys(item[0] || {}));
    let ind = 0;
    let idx = 0;
    let tname = '';
    let join, t1, t2, lastItem, lastTN;

    let item = tables[ind];
    if (!isArray(item)) _throw('FROM: table expected as first parameter');

    ind = checkNext(tables, item, tas, ind, tlen);
    lastTN = tas[idx++];
    t1 = fn(lastTN + '.', item);
    inputs.push(copyObj(t1));
    t1[pr] = {}; t1[amb] = {}; t1[empty] = nul(item);
    temps.push([null, extend(t1, t1[pr])]);
    lastItem = item;

    ind++;
    while (ind < tlen) {
        item = tables[ind];
        if (isArray(item)) {
            if (join && isArray(lastItem)) _throwErr(join, lastTN, lastItem);
            ind = checkNext(tables, item, tas, ind, tlen);
            tname += (tname ? ' + ' : '') + lastTN; lastTN = tas[idx++];
            t2 = fn(lastTN + '.', item);
            inputs.push(copyObj(t2));
            t2[empty] = nul(item);
            if (!join || join.cross) {
                t1 = checkCrossJoin(t1, t2, pr, amb, empty);
                temps.push([null, extend(t1, t1[pr])]); join = null;
            }
        }
        else if (isObject(item) && (item.join || item.ON)) {
            if (item.join) { if (join) _throwErr(join, lastTN, lastItem); join = item; }
            else {
                if (!(join && isArray(lastItem))) _throwErr(join, lastTN, lastItem);
                t1 = checkJoin(t1, t2, item.str1, item.str2, join, tname, lastTN, item.using, pr, amb, empty);
                temps.push([item.str1, extend(t1, t1[pr])]); join = null;
            }
        }
        else _throwErr(join, lastTN, lastItem);
        lastItem = item;
        ind++;
    }

    if (join) _throwErr(join, lastTN, lastItem);
    const tkeys = keys(extend(t1, t1[pr]));
    return { empty: t1[empty], tres: keys(t1), tkeys, ambs: keys(t1[amb]), inputs, temps };
};

const zip = (xs, ys) => xs.map((x, ind) => [x, ys[ind]]);

const pick2Fns = (t1, t2, fn) => 
    t1.simple && t2.simple ? pick2(t1.targets, t2.targets, fn) :
    t1.simple ? pick2KTS(t1.targets, zip(t2.targets, t2.sources), fn) :
    t2.simple ? pick2TSK(zip(t1.targets, t1.sources), t2.targets, fn) :
    pick2TS(zip(t1.targets, t1.sources), zip(t2.targets, t2.sources), fn);

const invert = obj => {
    const res = {};
    for (let key of keys(obj)) res[obj[key]] = key;
    return res;
};

const joinFn = (obj, table, columns, temps, index) => {
    if (index > len(temps) - 1) return { table };
    const tmp_obj = temps[index][1];
    const inv = invert(obj);
    const invt = invert(tmp_obj);
    const tmps = map(t => t[0])(drop(index, temps));
    const used = _uniq(append(columns, tmps));
    const fn = k => (obj[k] && obj[k] === tmp_obj[k]) || k === obj[tmp_obj[k]] || (inv[k] && invt[k]);
    const used_loc = filter (fn) (used);
    const targets = map (k => tmp_obj[k] ? k : invt[k]) (used_loc);
    const sources = map (k => obj[k] && tmp_obj[k] ? k : tmp_obj[k]) (targets);
    const simple = all ((t, ind) => t === sources[ind]) (targets);
    return { targets, sources, simple, table };
};

const executeJoin = (t1, t2, str1, str2, join, using, columns, temps, join_ind) => {
    const obj = temps[join_ind][1];
    let fn;

    if (join.semi) {
        const t = join.left ? t1 : t2;
        fn = t.simple ? _pick(t.targets) : pickTS(zip(t.targets, t.sources));
    }
    else fn = pick2Fns(t1, t2, using && join.fnplus ? join.fnplus(str1) : null);

    const table = join.fn({ fn, t1: t1.table, t2: t2.table, str1, str2, tgs1: t1.targets, tgs2: t2.targets });
    return joinFn(obj, table, columns, temps, join_ind + 1);
};

const checkNextInd = (tables, ind, tlen) => ind + 1 < tlen && isString(tables[ind + 1]) ? ind + 1 : ind;
const _uniq = strs => [...new Set(strs)];

const joinTables = (tables, columns, inputs, temps) => {
    const tlen = len(tables);
    let ind = 0;
    let idx = 0;
    let join_ind = 1;
    let item = tables[0];
    let join, t1, t2;

    if (len(temps) === 1) return _select(intersect(keys(inputs[idx]), columns)) (item);
    ind = checkNextInd(tables, ind, tlen);
    t1 = joinFn(inputs[idx++], item, columns, temps, join_ind);

    ind++;
    while (ind < tlen) {
        item = tables[ind];
        if (isArray(item)) {
            ind = checkNextInd(tables, ind, tlen);
            t2 = joinFn(inputs[idx++], item, columns, temps, join_ind, false);
            if (!join || join.cross) {
                t1 = executeJoin(t1, t2, null, null, { fn: _prodSQL }, false, columns, temps, join_ind);
                join = null; join_ind++;
            }
        }
        else {
            if (item.join) join = item;
            else {
                t1 = executeJoin(t1, t2, item.str1, item.str2, join, item.using, columns, temps, join_ind);
                join = null; join_ind++;
            }
        }
        ind++;
    }
    return t1.table;
};

const checkGroupBy = (groupby, strs, sa, input, ambs, selected_columns) => {
    const err_str = `GROUP_BY`;
    const gc = checkOrder(groupby, pgFn, err_str);
    strColsCheck(gc, err_str);
    checkSub(gc, input, ambs, err_str);
    checkSub(append(strs, sa), gc, [], selected_columns, ' in GROUP_BY');
    return gc;
};

const checkWhere = (where, input, ambs, from) => {
    if (!isFunction(where.pred)) _throw(`predicate expected (${ from })`);
    const fnStr = where.pred + '';
    const char = fnStr.match(/[=(]/)[0];
    const param = fnStr.match(char === '=' ? /\b\w+\b/ : /(?<=\(\s*)\b\w+\b/);
    if (!param) _throw(`parameter expected in predicate of ${ from }`);
    const object = '(?<=\\b' + param[0] + '\\s*';

    const dotProp = object + '\\.\\s*)\\w+\\b';
    
    const left = '\\[\\s*[\'\"\`])';
    const right = '(?=[\'\"\`]\\s*\\])';
    const indProp = object + left  + '[^\'"`]*' + right ;

    let props = fnStr.match(new RegExp(dotProp + '|' + indProp, 'g')) || [];
    props = trimCols(props);
    for (let str of props) strNulCheck(str, from);
    checkSub(props, input, ambs, from);
    return props;
};

const checkUniqness = columns => {
    const _columns = map (fnToAlias) (columns);
    if (!isUniq(_columns)) {
        const set = new Set();
        let duplicate;
        let ind = 0;

        for (let column of _columns) {
            if (set.has(column)) { duplicate = column; break; }
            set.add(column);
            ind++;
        }
        _throw(`column ${ duplicate } (SELECT ${ ind + 1 }${ isString(columns[ind]) ? '' : ' AS' }) already exists`);
    }
};

const checkNotSub = (keys, sel_cols) => {
    const fns = filter (c => !isString(c))(sel_cols);
    const columns = map (fnToAlias) (fns);
    if (!nul(intersect(columns, keys))) {
        const set = new Set(keys);
        let duplicate;

        for (let fn of fns) {
            if (set.has(fnToAlias(fn))) { duplicate = fn; break; }
        }
        _throw(`column ${ fnToAlias(duplicate) } (SELECT ${ duplicate.ind } AS) already exists [in table]`);
    }
};

const checkSelects = (strs, tkeys, ambs, selected_columns, any_str) => {
    if (any_str) { for (let str of strs) strNulCheck(str, selected_columns); }
    checkUniqness(selected_columns);
    checkNotSub(append(tkeys, ambs), selected_columns);
    if (any_str) checkSub(strs, tkeys, ambs, selected_columns);
};

const _selectSQL = (params, distinct, top, n) => {
    let index = params.findIndex(item => !(isString(item) || item.isFn));
    if (index < 1) {
        if (index > -1) _throw('no column selected');
        index = len(params);
    }
    const selected_columns = take(index, params);
    const clauses = drop(index, params);

    let last_ind = -1;
    let ord, from, where, groupby, having, orderby;
    for (let clause of clauses) {
        ord = clause.select_ind;
        if (ord < last_ind) _throw('SELECT clauses: wrong order');
        if (ord === last_ind) _throw('SELECT clauses: duplicates');
        if (ord === 0) from = clause;
        else if (ord === 1) where = clause;
        else if (ord === 2) groupby = clause;
        else if (ord === 3) having = clause;
        else if (ord === 4) orderby = clause;
        else _throw('SELECT clauses: invalid clause');
        last_ind = ord;
    }

    if (!from) _throw('FROM must be supplied');
    let tables = from.tables;

    let any_Fn;
    let ind = 1;
    const fns = [];
    for (let column of selected_columns) {
        if (!isString(column)) {
            if (!(isObject(column) && column.isFn)) _throw(`invalid argument (SELECT ${ ind })`);
            column.ind = ind; any_Fn = true; fns.push(column);
        }
        ind++;
    }

    let aggr_win_aliases;
    if (any_Fn) aggr_win_aliases = checkAliases(fns);

    let any_str, any_scal, any_aggr, any_win, any_winaggr;
    let strs = [];
    const scals = [], aggrs = [], wins = [], winaggrs = [];
    for (let column of selected_columns) {
        if (isString(column)) { any_str = true; strs.push(trim(column)); }
        else {
            if (column.scal) { any_scal = true; scals.push(column); }
            else if (column.aggr) { any_aggr = true; aggrs.push(column); }
            else {
                any_win = true; wins.push(column);
                if (column.winaggr) { any_winaggr = true; winaggrs.push(column); }
            }
        }
    }

    let star;
    if (index === 1 && any_str && strs[0] === '*') star = true;

    if ((any_str || any_scal) && (any_aggr || any_winaggr) && !groupby) _throw('GROUP_BY required');
    if (having && !groupby) _throw('GROUP_BY required [before HAVING]');

    const { empty, tres, tkeys, ambs, inputs, temps } = checkJoinTables(tables);
    if (empty) return [];
    if (!star) checkSelects(strs, tkeys, ambs, selected_columns, any_str);

    let input = copy(tkeys), whc = [], gc = [], hc = [], oc = [];
    let scal_names = [], aggr_names = [], win_names = [], sa = [], aa = [], wa = [];
    if (where) whc = checkWhere(where, input, ambs, 'WHERE');
    if (any_scal) ({ scal_names, sa } = checkScal(scals, input, ambs));
    const sa_input = copy(input);

    if (star) strs = tres;
    const _grouping = groupby || any_aggr;
    if (_grouping) {
        if (groupby) {
            const sel_cols = star ? tres : selected_columns;
            gc = checkGroupBy(groupby, strs, sa, input, ambs, sel_cols);
        }
        ({ aggr_names, aa, input } = checkAggr(aggrs, input, ambs, gc, aggr_win_aliases));
        if (having) hc = checkWhere(having, input, [], 'HAVING');
    }

    const grouping = _grouping || any_winaggr;
    input = any_winaggr && !_grouping ? [] : input;
    if (any_win) ({ win_names, wa } = checkWin(wins, input, sa_input, ambs, grouping, aggr_win_aliases));
    if (orderby) oc = checkOrderBy(orderby, input, grouping ? [] : ambs);

    const next_names = _grouping ? aggr_names : win_names;
    const used = concat(strs, scal_names, next_names, gc, whc, _grouping ? [] : oc);
    const aliases = append(sa, _grouping ? aa : wa);
    const columns = difference(used, aliases);
    let res = joinTables(tables, columns, inputs, temps);


    if (where) res = whereFn(where, res);
    if (any_scal) executeScalars(res, scals);

    if (grouping) {
        const used = difference(concat(strs, sa, hc, win_names, oc), wa);
        const columns = any_aggr || any_winaggr ? concat(used, aggrs, winaggrs) : used;
        const partitions = groupby ? groupbyFn(groupby, res) : [len(res)];
        res = executeAggregates(partitions, res, columns);
        if (having) res = whereFn(having, res);
    }

    if (any_win) executeWins(res, wins);

    const sel_columns = any_Fn ? map (fnToAlias) (selected_columns) : strs;
    if (!grouping && distinct && !nul(res)) res = getDistinct(sel_columns, res);
    if (orderby) orderbyFn(orderby, res);
    if (top) res = take(n, res);
    return select(sel_columns) (res);
};

const trimCols = cols => map(str => isString(str) ? trim(str) : str) (cols);

const fulljoinFn = col => (res, obj1, obj2) => {
    if (obj1[col] == null) res[col] = obj2[col] == null ? null : obj2[col];
};

const rightjoinFn = col => (res, _, obj2) => {
    res[col] = obj2[col] == null ? null : obj2[col];
}

////

export const JOIN = { join: true, fn: hashjoin };
export const INNER_JOIN = { join: true, fn: hashjoin };
export const CROSS_JOIN = { join: true, cross: true };
export const LEFT_JOIN = { join: true, fn: leftjoin };
export const RIGHT_JOIN = { join: true, fn: rightjoin, fnplus: rightjoinFn };
export const FULL_JOIN = { join: true, fn: fulljoin, fnplus: fulljoinFn };
export const LEFT_SEMI_JOIN = { join: true, semi: true, left: true, fn: leftsemijoin };
export const RIGHT_SEMI_JOIN = { join: true, semi: true, fn: rightsemijoin };
export const LEFT_ANTI_JOIN = { join: true, semi: true, left: true, anti: true, fn: leftantijoin };
export const RIGHT_ANTI_JOIN = { join: true, semi: true, anti: true, fn: rightantijoin };

export const ON = (str1, str2) => {
    if (isString(str1)) str1 = trim(str1);
    if (isString(str2)) str2 = trim(str2);
    return { ON: true, str1, str2 };
};

export const USING = str => {
    if (isString(str)) str = trim(str);
    return { ON: true, str1: str, str2: str, using: true };
};

export const UNBOUNDED_PRECEDING = { up: true };
export const PRECEDING = { prec: true };
export const CURRENT_ROW = { cr: true };
export const FOLLOWING = { fol: true };
export const UNBOUNDED_FOLLOWING = { uf: true };

export const ROWS_BETWEEN = (...frame_boundaries) =>
    ({ frame_boundaries, over_ind: 2, rows: true, fn: executeRows });
export const RANGE_BETWEEN = (...frame_boundaries) =>
    ({ frame_boundaries, over_ind: 2, range: true, fn: executeRange });
export const GROUPS_BETWEEN = (...frame_boundaries) =>
    ({ frame_boundaries, over_ind: 2, groups: true, fn: executeGroups });


export const ROWS = (offset_fs, frame_start) => frameAbbr(ROWS_BETWEEN, offset_fs, frame_start);
export const RANGE = (offset_fs, frame_start) => frameAbbr(RANGE_BETWEEN, offset_fs, frame_start);
export const GROUPS = (offset_fs, frame_start) => frameAbbr(GROUPS_BETWEEN, offset_fs, frame_start);

export const PARTITION_BY = (...columns) => {
    columns = trimCols(columns);
    return { columns, over_ind: 0 };
}
export const OVER = (...clauses) => ({ clauses, OVER: true });

export const ROW_NUMBER = (...ps) => {
    const rn_obj = RN(ps, executeRowNumber);
    rn_obj.row_num = true;
    return rn_obj;
};
export const RANK = (...ps) => RN(ps, executeRank);
export const DENSE_RANK = (...ps) => RN(ps, executeRank, true);
export const PERCENT_RANK = (...ps) => RN(ps, executePercentRank);
export const CUME_DIST = (...ps) => RN(ps, executeCumeDist);
export const NTILE = (...ps) => RN(ps, executeNTile);

export const LEAD = (...ps) => LEAD_LAG(ps, 1);
export const LAG = (...ps) => LEAD_LAG(ps, -1);

export const FIRST_VALUE = (...ps) => VALUE(ps, 0);
export const LAST_VALUE = (...ps) => VALUE(ps, -1);
export const NTH_VALUE = (...ps) => VALUE(ps, 1);

export const WITHIN_GROUP = orderby => ({ orderby, wg: true, wg_ind: 1 });

export const PERCENTILE_CONT = (...ps) => PERCENTILE(ps, true);
export const MEDIAN = (...ps) => median(ps); 
export const PERCENTILE_DISC = (...ps) => PERCENTILE(ps);

export const COUNT = (...ps) => aggr(ps, _COUNT);
export const SUM = (...ps) => aggr(ps, _SUM);
export const AVG = (...ps) => aggr(ps, _AVG);
export const MIN = (...ps) => aggr(ps, _MIN);
export const MAX = (...ps) => aggr(ps, _MAX);

export const COUNT_DISTINCT = (...ps) => aggrDist(ps, _COUNT);
export const SUM_DISTINCT = (...ps) => aggrDist(ps, _SUM);
export const AVG_DISTINCT = (...ps) => aggrDist(ps, _AVG);

export const AS = str => ({ alias: true, alias_str: str });

export const ID = (...ps) => scalar(x => x, ps);
export const LEN = (...ps) => scalar(_len, ps);
export const ROUND = (...ps) => scalar(_round, ps);

export const ADD = (...ps) => scalar(add, ps);
export const SUB = (...ps) => scalar(sub, ps);
export const DIV = (...ps) => scalar(div, ps);
export const MUL = (...ps) => scalar(mul, ps);

export const FROM = (...tables) => ({ tables, select_ind: 0});
export const WHERE = pred => ({ pred, select_ind: 1 });

export const GROUP_BY = (...columns) => {
    columns = trimCols(columns);
    return { columns, select_ind: 2 };
};

export const HAVING = pred => ({ pred, select_ind: 3});

export const ASC = { ord_ind: 0 };
export const DESC = { ord_ind: 1 };
export const NOCASE_ASC = { ord_ind: 2 };
export const NOCASE_DESC = { ord_ind: 3 };
export const ASC_LOC = { ord_ind: 4 };
export const DESC_LOC = { ord_ind: 5 };

export const ORDER_BY = (...columns) => {
    columns = trimCols(columns);
    return { columns, select_ind: 4, over_ind: 1, wg_ind: 0 };
};

export const SELECT = (...columns) => _selectSQL(columns);
export const SELECT_TOP = n => (...columns) => _selectSQL(columns, false, true, n);
export const SELECT_DISTINCT = (...columns) => _selectSQL(columns, true);
export const SELECT_DISTINCT_TOP = n => (...columns) => _selectSQL(columns, true, true, n);

export const createFn = fn => (...ps) => scalar(fn, ps);