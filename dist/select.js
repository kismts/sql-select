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


const checkSub = (sub, xs, ambs, from, clause) => {
    if (!isSubset(sub, xs)) {
        const diffs = difference(sub, xs);
        const ambiguous = intersect(diffs, ambs);
        const wrongColumn = len(ambiguous) ? ambiguous[0] : diffs[0];
        if (isObject(from)) from = `SELECT ${ from.ind }`;
        else if (isArray(from))
            from = `SELECT ${ (map (fnToAlias) (from)).indexOf(wrongColumn) + 1 }`;
        if (len(ambiguous))
            _throw(`column ${ wrongColumn } (${ from }) ambiguous${ clause ? clause : ''}`);
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

const checkProp = p => {
    if (isString(p)) p = trim(p);
    else if (isObject(p)) p['nested'] = true;
    return p;
};

const _AS = ps => last(ps) && last(ps).alias ? last(ps) : null;
const _err = (num, plen, err) => plen < num ? 'more args expected' :
                                 plen > num ? 'too many args' : err;

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

const _count = (xs, prop, ind, n) => {
    if (prop === '*') return n;
    const fn = aFn(prop);
    const to = ind + n;
    let count = 0;

    while (ind < to) if (fn(xs[ind++]) != null) count++;
    return count;
};

const _sum = (xs, prop, ind, n) => {
    const fn = aFn(prop);
    const to = ind + n;
    let sum = 0;
    let count = 0;
    let item;

    while (ind < to) {
        item = fn(xs[ind++]);
        if (item != null) { sum += item; count++; }
    }
    return count ? sum : null;
};

const _avg = (xs, prop, ind, n) => {
    const fn = aFn(prop);
    const to = ind + n;
    let sum = 0;
    let count = 0;
    let item;

    while (ind < to) {
        item = fn(xs[ind++]);
        if (item != null) { sum += item; count++; }
    }
    return count ? sum / count : null;
};

const _min = (x, y) => x != null && y != null ? (y < x ? y : x) : x == null ? y : x;
const _max = (x, y) => x != null && y != null ? (y > x ? y : x) : x == null ? y : x; 

const minmax = (xs, prop, ind, n, fnn) => {
    const fn = aFn(prop);
    if (n === 1) return fn(xs[ind]);
    const to = ind + n;
    let res = fn(xs[ind++]);

    while (ind < to) res = fnn(fn(xs[ind++]), res);
    return res;
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

const aggr = (ps, fn, fnn) => {
    let num = 1;
    const AS = _AS(ps);
    if (AS) num++;
    const err = _err(num, len(ps), null);
    const prop = checkProp(ps[0]);
    const dist = typeof fnn === 'boolean' ? fnn : false;
    const fn1 = (obj, arr, start, n) => {
        const res = fn(arr, prop, start, n, fnn);
        return AS ? obj[AS['alias_str']] = res : res;    
    };
    const fn2 = (obj, arr, start, n) => {
        arr = uniq(arr, aFn(prop), start, n);
        return fn1(obj, arr, 0, len(arr));
    };
    return { aggregate: true, aggr: true, prop, AS, isFn: true, err,
                    count: fn === _count, fn: dist ? fn2 : fn1 };
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

const _isAggr = ps =>
    any(p => isObject(p) && (p.aggr || (p.scalar && _isAggr(p.props)))) (ps);

const isAggr = (ps, aa) => {
    return any(p => (isString(p) && isElem(p, aa)) ||
                    (isObject(p) && (p.aggr || (p.scalar && isAggr(p.props, aa))))) (ps);
};

const checkScalar = (fn, aa, alias) => {
    const ps = init(fn.props);

    if (isAggr(ps, aa)) { fn.aggr = true; aa.push(alias); return; }
    fn.scal = true;
};


const checkAliases = fns => {
    const aa = [];
    let alias;

    for (let fn of fns) {
        if (fn.scalar) {
            alias = checkScalarAliases(fn);
            checkScalar(fn, aa, alias);
        }
        else {
            alias = checkAggregateAliases(fn);
            aa.push(alias);
        }
    }
    return aa;
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

const _checkProp = (p, ind) => {
    if (isString(p) ? nul(p) : !(isObject(p) && p.isFn)) {
        _throw(`invalid parameter (SELECT ${ ind })`);
    }
};

const checkAggregateFn = (fn, ind) => {
    if (fn.nested && fn.AS) _throw(`nested AS (SELECT ${ ind })`);
    if (fn.err) _throw(`${ fn.err } (SELECT ${ ind })`);
    _checkProp(fn.prop, ind);
};

const isNested = (aggr, p, aliases) => aggr && isElem(p, aliases);


const scalInd = 0, startInd = 1, restInd = 2;
const checkNesting = (fn, root, input, ambs, ind, aliases) => {
    const res = [[],[],[]];
    if (fn.aggregate) {
        if (root === scalInd) _throw(`nesting error (SELECT ${ ind })`);
        root = fn.aggr ? scalInd : restInd;
        checkAggregateFn(fn, ind);
        const prop = fn.prop;
        if (isString(prop) && !(fn.count && prop === '*')) {
            if (isNested(fn.aggr, prop, aliases))
                _throw(`nesting error (SELECT ${ ind + ' ' + prop })`);
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

const executeAggregates = (partitions, arr, columns) => {
    const plen = len(partitions);
    const res = Array(plen);
    let ind = -1;
    while (++ind < plen) res[ind] = {};

    const fn = col => {
        const fnn = isString(col) ? (obj, arr, start) => obj[col] = arr[start][col] :
                                    col.aggregate ? col.fn.bind(col) : _scalarAggrFn(col);
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

const checkCrossJoin = (t1, t2, pr, amb, empty) =>
    t1[empty] || t2[empty] ? (t1[empty] = true, t1) : mergeObjs(t1, t2, pr, amb, empty);

const checkJoin = (t1, t2, str1, str2, ta1, ta2, using, pr, amb, empty) => {
    if (t1[empty] || t2[empty]) return (t1[empty] = true, t1);
    const pre = using ? 'USING ' : 'ON ';
    stringCheck(str1, `FROM ${ pre + ta1 }`);
    stringCheck(str2, `FROM ${ pre + ta2 }`);
    if (!(t1[str1] || t1[pr][str1]))
        checkSub([str1], keys(t1), keys(t1[amb]), `FROM ${ pre + ta1 }`);
    if (!t2[str2]) checkSub([str2], keys(t2), [], `FROM ${ pre + ta2 }`);
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
    _throw(fromStr + (isArray(lastItem) ? `` : `after ON/USING `) +
                                    afterStr + `table or JOIN expected`);
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
                t1 = checkJoin(t1, t2, item.str1, item.str2,
                                    tname, lastTN, item.using, pr, amb, empty);
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
    const fn = k =>
        (obj[k] && obj[k] === tmp_obj[k]) || k === obj[tmp_obj[k]] || (inv[k] && invt[k]);
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

    const table = join.fn({ fn, t1: t1.table, t2: t2.table, str1, str2,
                                            tgs1: t1.targets, tgs2: t2.targets });
    return joinFn(obj, table, columns, temps, join_ind + 1);
};

const checkNextInd = (tables, ind, tlen) =>
    ind + 1 < tlen && isString(tables[ind + 1]) ? ind + 1 : ind;
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
                t1 = executeJoin(t1, t2, null, null, { fn: _prodSQL },
                                            false, columns, temps, join_ind);
                join = null; join_ind++;
            }
        }
        else {
            if (item.join) join = item;
            else {
                t1 = executeJoin(t1, t2, item.str1, item.str2, join, item.using,
                                                        columns, temps, join_ind);
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
        _throw(`column ${ duplicate } (SELECT ${ ind + 1 }${
                                  isString(columns[ind]) ? '' : ' AS' }) already exists`);
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
        _throw(`column ${ fnToAlias(duplicate) } (SELECT ${
                                            duplicate.ind } AS) already exists [in table]`);
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
            if (!(isObject(column) && column.isFn))
                _throw(`invalid argument (SELECT ${ ind })`);
            column.ind = ind; any_Fn = true; fns.push(column);
        }
        ind++;
    }

    let aggr_aliases;
    if (any_Fn) aggr_aliases = checkAliases(fns);

    let any_str, any_scal, any_aggr;
    let strs = [];
    const scals = [], aggrs = [];
    for (let column of selected_columns) {
        if (isString(column)) { any_str = true; strs.push(trim(column)); }
        else {
            if (column.scal) { any_scal = true; scals.push(column); }
            else { any_aggr = true; aggrs.push(column); }
        }
    }

    let star;
    if (index === 1 && any_str && strs[0] === '*') star = true;

    if ((any_str || any_scal) && any_aggr && !groupby) _throw('GROUP_BY required');
    if (having && !groupby) _throw('GROUP_BY required [before HAVING]');

    const { empty, tres, tkeys, ambs, inputs, temps } = checkJoinTables(tables);
    if (empty) return [];
    if (!star) checkSelects(strs, tkeys, ambs, selected_columns, any_str);

    let input = copy(tkeys), whc = [], gc = [], hc = [], oc = [];
    let scal_names = [], aggr_names = [], sa = [], aa = [];
    if (where) whc = checkWhere(where, input, ambs, 'WHERE');
    if (any_scal) ({ scal_names, sa } = checkScal(scals, input, ambs));

    if (star) strs = tres;
    const grouping = groupby || any_aggr;
    if (grouping) {
        if (groupby) {
            const sel_cols = star ? tres : selected_columns;
            gc = checkGroupBy(groupby, strs, sa, input, ambs, sel_cols);
        }
        ({ aggr_names, aa, input } = checkAggr(aggrs, input, ambs, gc, aggr_aliases));
        if (having) hc = checkWhere(having, input, [], 'HAVING');
    }

    if (orderby) oc = checkOrderBy(orderby, input, grouping ? [] : ambs);
    const used = concat(strs, scal_names, aggr_names, gc, whc, grouping ? [] : oc);
    const aliases = grouping ? append(sa, aa) : sa;
    const columns = difference(used, aliases);
    let res = joinTables(tables, columns, inputs, temps);


    if (where) res = whereFn(where, res);
    if (any_scal) executeScalars(res, scals);

    if (grouping) {
        const used = concat(strs, sa, hc, oc);
        const columns = any_aggr ? concat(used, aggrs) : used;
        const partitions = groupby ? groupbyFn(groupby, res) : [len(res)];
        res = executeAggregates(partitions, res, columns);
        if (having) res = whereFn(having, res);
    }

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


export const ON = (str1, str2) => {
    if (isString(str1)) str1 = trim(str1);
    if (isString(str2)) str2 = trim(str2);
    return { ON: true, str1, str2 };
};

export const USING = str => {
    if (isString(str)) str = trim(str);
    return { ON: true, str1: str, str2: str, using: true };
};


export const COUNT = (...ps) => aggr(ps, _count, false);
export const SUM = (...ps) => aggr(ps, _sum, false);
export const AVG = (...ps) => aggr(ps, _avg, false);
export const MIN = (...ps) => aggr(ps, minmax, _min);
export const MAX = (...ps) => aggr(ps, minmax, _max);

export const COUNT_DISTINCT = (...ps) => aggr(ps, _count, true);
export const SUM_DISTINCT = (...ps) => aggr(ps, _sum, true);
export const AVG_DISTINCT = (...ps) => aggr(ps, _avg, true);

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
    return { columns, select_ind: 4 };
};

export const SELECT = (...columns) => _selectSQL(columns);
export const SELECT_TOP = n => (...columns) => _selectSQL(columns, false, true, n);
export const SELECT_DISTINCT = (...columns) => _selectSQL(columns, true);
export const SELECT_DISTINCT_TOP = n => (...columns) => _selectSQL(columns, true, true, n);

export const createFn = fn => (...ps) => scalar(fn, ps);