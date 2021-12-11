





export const input1 = [
    { id: 1, name: 'A' },
    { id: 2, name: 'B' },
    { id: 3, name: 'C' },
    { id: null, name: 'D' },
    { id: 3, name: 'E' },
    { id: 4, name: 'F' },
    { id: null, name: 'G' }
]



export const input2 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 },
    { id: 4, age: 22 },
    { id: null, age: 33 },
    { id: 5, age: 44 },
    { id: 6, age: 55 },
    { id: null, age: 66 }
]

export const input20 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 },
    { id: 4, age: 22 },
    { id: 4, age: 25 },
    { id: null, age: 33 },
    { id: 5, age: 44 },
    { id: 6, age: 55 },
    { id: 7, age: 60 },
    { id: null, age: 66 },
    { id: null, age: 50 }
]


export const input3 = [
    { id: 2, city: 'London' },
    { id: 3, city: 'Paris' },
    { id: 4, city: 'Berlin' },
    { id: 5, city: 'Rome' },
    { id: 7, city: 'Budapest' }
]

export const input11 = [
    { id: 1, name: 'A' },
    { id: 2, name: 'B' }
]

export const input22 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 }
]

export const input33 = [
    { id: 2, city: 'London' },
    { id: 3, city: 'Paris' },
]


/////////////////////////////////////////////


export const input4 = [
    { id: 1, name: 'A', age: 8, type: 1 },
    { id: 2, name: 'B', age: 3, type: 3 },
    { id: 3, name: 'C', age: 5, type: 4 },
]


export const input5 = [
    { id: 1, city: 'London', age: 15, size: 8 },
    { id: 1, city: 'Paris', age: 19, size: 4 },
    { id: 1, city: 'Rome', age: 33, size: null },
    { id: 1, city: 'Paris', age: null, size: 7 },
    { id: 1, city: 'London', age: 10, size: 8 },
    { id: 1, city: 'Paris', age: 99, size: 4 },
    { id: 1, city: 'London', age: null, size: 8 },
    { id: 1, city: 'Rome', age: 9, size: 4 },
    { id: 1, city: 'Berlin', age: 15, size: 5 },
    { id: 1, city: 'Paris', age: 8, size: null },
    { id: 1, city: 'London', age: 1, size: 9 },
    { id: 1, city: 'Paris', age: 19, size: 4 },

    { id: 2, city: 'Berlin', age: 8, size: 2 },
    { id: 2, city: 'Budapest', age: 2, size: 5 },
    { id: 2, city: 'Madrid', age: null, size: 2 },
    { id: 2, city: 'Rome', age: 1, size: null },
    { id: 2, city: 'Berlin', age: 8, size: 3 },
    { id: 2, city: 'Budapest', age: null, size: 11 },
    { id: 2, city: 'Berlin', age: 3, size: 1 },
    { id: 2, city: 'Budapest', age: 7, size: null },
    { id: 2, city: 'Madrid', age: 11, size: 22 },
    { id: 2, city: 'Budapest', age: 5, size: null },
    { id: 2, city: 'Berlin', age: null, size: 7 },
    { id: 2, city: 'Madrid', age: 2, size: 3 },

    { id: 3, city: 'London', age: null, size: 2 },
    { id: 3, city: 'Budapest', age: 2, size: null },
    { id: 3, city: 'Berlin', age: 8, size: 1 },
    { id: 3, city: 'Rome', age: 1, size: 11 },
    { id: 3, city: 'Berlin', age: 9, size: 3 },
    { id: 3, city: 'Budapest', age: 3, size: null },
    { id: 3, city: 'Rome', age: null, size: 2 },
    { id: 3, city: 'Rome', age: 2, size: 10 },
    { id: 3, city: 'Berlin', age: 15, size: 3 },
    { id: 3, city: 'Budapest', age: 4, size: 3 },
    { id: 3, city: 'Berlin', age: 8, size: 2 },
    { id: 3, city: 'London', age: 2, size: null },
]


const createT = (db, name, createTableStr, insertStr, table, time) => {
    if (time) console.time(`${ name } creation time`);

    db.prepare(createTableStr).run();
    const stmt = db.prepare(insertStr);
    for(const row of table) stmt.run(row);

    if (time) console.timeEnd(`${ name } creation time`);
};

const isString = x => typeof x === 'string';
const isNumber = x => typeof x === 'number';
const map = fn => xs => xs.map(fn);
const transpose = xs => xs.length ? xs[0].map((_, colIndex) => xs.map(row => row[colIndex])) : [];
const join = (sep, arr) => arr.join(sep);

const getType = values => {
    for (let v of values) {
        if (isString(v)) return 'TEXT';
        if (isNumber(v)) return 'INT';
    }
    return 'INT';
};

export const createTable = (db, input, name, time) => {
    const _columns = Object.keys(input[0]);
    const last = _columns.length - 1;
    const fn = obj => map(p => obj[p]) (_columns);
    const matrix = transpose(map (fn) (input));
    const _types = map ((c, ind) => `${ c } ${ getType(matrix[ind]) } NULL${ ind < last ? ',' : '' }`) (_columns);
    const types = join('\n', _types);
    const columns = join('\n', map ((c, ind) => `@${ c }${ ind < last ? ',' : '' }`) (_columns))
        
    const createTableStr = `CREATE TABLE IF NOT EXISTS ${ name } (${ types });`;
    const insertStr = `INSERT INTO ${ name } VALUES (${ columns })`;
    createT(db, name, createTableStr, insertStr, input, time);
};



