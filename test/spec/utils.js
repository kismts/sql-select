const _slice = (xs, start, end) => xs.slice(start, end);
const len = xs => xs.length;
const keys = Object.keys;
const nul = xs => len(xs) === 0;
const map = fn => xs => xs.map(fn);
const all = pred => xs => xs.every(pred);
const append = (xs, ys) => xs.concat(ys);
const minFn = (x, y) => y < x ? y : x;
const take = (n, xs) => _slice(xs, 0, n < 0 ? 0 : n);


const _gt = fn => (x, y) => fn(x) > fn(y);
const best = (xs, fn) => {
    const xlen = len(xs);
    let ind = -1;
    let res = xs[++ind];

    while (++ind < xlen) {
        res = fn(xs[ind], res) ? xs[ind] : res;
    }
    return res;
};

const maxLen = xss => len(xss) ? len(best (xss, _gt(len))) : NaN;
const transpose = xs => len(xs) ? xs[0].map((_, colIndex) => xs.map(row => row[colIndex])) : [];
const isNum_Null = num => typeof num === 'number' || num == null;
const repeat = (x, n) => x.repeat(n);
const unlines = arr => arr.join('\n');
const unchars = arr => arr.join('');


export const drawTable = (table, config) => {
    const tlen = len(table);
    const _columns = tlen ? keys(table[0]) : [''];
    const columns = nul(_columns) ? [''] : _columns;
    const fn = nul(_columns) ? _ => [''] : obj => map(p => obj[p]) (columns);
    const matrix = map (fn) (table);
    const nums = map (row => all(isNum_Null)(row)) (transpose(matrix));
    const str_matrix = map(map(String)) (matrix);
    const str_table = append([columns], str_matrix);
    const widthsFn = (row, ind) => minFn(maxLen(row), config && config[ind+1] != null ? config[ind+1] : Infinity);
    const widths = map (widthsFn) (transpose(str_table));
    const end = len(columns) - 1;
    const lineFn = (_, ind) => '+-' + repeat('-', widths[ind]) + (ind < end ? '-' : '-+');
    const line = unchars (map (lineFn) (columns)) + '\n';
    const cell = (txt, ind) => {
        txt = take(widths[ind], txt);
        const ws = repeat(' ', widths[ind] - len(txt));
        return '| ' + (nums[ind] ? ws + txt : txt + ws) + (ind < end ? ' ' : ' |');
    };

    const header = line + unchars(map (cell) (columns)) + '\n' + line;
    const body = tlen ? unlines (map(row => unchars(map (cell) (row))) (str_matrix)) + '\n' : '';
    const footer = tlen >= 50 ? header : line;
    const numOfrows = `${ tlen } row${ tlen > 1 ? 's' : ''  } selected`;
    return '\n\n' + header + body + footer + numOfrows + '\n\n';
};

export const htmlTable = (table, config) => {
    const caption = config && config.caption;
    const _class = config && config.class;
    const tlen = len(table);
    const columns = tlen ? keys(table[0]) : [];
    const classes = _class ? map (col => ` class="item-${ col.split(/\s+/).join('-') }"`) (columns) : null;
    const matrix = map (obj => map(p => obj[p]) (columns)) (table);
    const str_matrix = map(map(String)) (matrix);
    const str_table = append([columns], str_matrix);
    const th = (txt, ind) => `<th scope="col"${ _class ? classes[ind] : '' }>` + txt + '</th>';
    const td = (txt, ind) => `<td${ _class ? classes[ind] : '' }>` + txt + '</td>';
    const start = ind => ind === 0 ? '<thead>' : ind === 1 ? '<tbody>' : '';
    const end = ind => ind === 0 ? '</thead>' : ind === tlen ? '</tbody>' : '';
    const fn = (row, ind) => start(ind)  + '<tr>' + unchars(map (ind ? td : th) (row)) + '</tr>' + end(ind);
    const body = unlines (map(fn) (str_table));
    return '<table>\n' + (caption ? '<caption>' + caption + '</caption>\n' : '') + body + '\n</table>';
};