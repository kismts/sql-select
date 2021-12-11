import { SELECT, FROM, JOIN, ON, ORDER_BY, ADD, AS, WHERE, OVER,
    SUM, GROUP_BY, HAVING } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')



describe('aggregate (without groupby) + win functions:', () => {
    it(`aggr + win`, () => {
        const table = `
+-----+------+-------+
| sum | wsum | wsum2 |
+-----+------+-------+
|  37 |   37 |    37 |
+-----+------+-------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
            SUM(SUM('size'), OVER(), AS('wsum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`aggr + win + aw_scal`, () => {
        const table = `
+-----+------+-----+------+------+------+
| sum | wsum | add | add2 | add3 | add4 |
+-----+------+-----+------+------+------+
|  37 |   37 |  74 |   74 |   74 |   74 |
+-----+------+-----+------+------+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
            ADD('sum', 'wsum', AS('add')),
            ADD(SUM('size'), SUM('sum', OVER()), AS('add2')),
            ADD('sum', SUM('sum', OVER()), AS('add3')),
            ADD(SUM('size'), 'wsum', AS('add4')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));
    });

    it(`aggr + win + aw_scal`, () => {
        const table = `
+------+-----+-----+------+------+
| wsum | add | sum | add2 | add3 |
+------+-----+-----+------+------+
|   37 |  74 |  37 |   74 |  148 |
+------+-----+-----+------+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('sum', OVER(), AS('wsum')),
            ADD('sum', 'wsum', AS('add')),
            SUM('size', AS('sum')),
            ADD(SUM('size'), 'wsum', AS('add2')),
            ADD(SUM('size'), 'wsum', ADD(SUM('size'), 'wsum'), AS('add3')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });



    it(`nesting`, () => {
const table = `
+------+-------+
| wsum | wsum2 |
+------+-------+
|   74 |   148 |
+------+-------+
1 row selected`;
        
        const res =
        SELECT(
            SUM(ADD(SUM('size'), SUM('size')), OVER(), AS('wsum')),
            SUM(ADD(SUM('size'), SUM('size'), ADD(SUM('size'), SUM('size'))), OVER(), AS('wsum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(SUM(size) + SUM(size)) OVER() AS wsum,
            SUM(SUM(size) + SUM(size) + (SUM(size) + SUM(size))) OVER() AS wsum2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY wsum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });
});

describe('groupby (without aggregates) + win functions:', () => {
    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+-----+------+
| city     | name | size | wsum | add | add2 |
+----------+------+------+------+-----+------+
| Berlin   | A    |    5 |   45 |  50 |   95 |
| Berlin   | B    |    7 |   45 |  52 |   97 |
| Budapest | B    |    5 |   45 |  50 |   95 |
| Budapest | B    |   11 |   45 |  56 |  101 |
| London   | A    |    8 |   45 |  53 |   98 |
| London   | A    |    9 |   45 |  54 |   99 |
+----------+------+------+------+-----+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            ADD('wsum', 'size', AS('add')),
            ADD('wsum', 'size', SUM('size', OVER()) , AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum,
            SUM(size) OVER() + size AS 'add',
            SUM(size) OVER() + size + SUM(size) OVER() AS 'add2'
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING size > 3
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
const table = `
+----------+------+------+------+-----+------+------+
| city     | name | size | wsum | asd | add1 | add2 |
+----------+------+------+------+-----+------+------+
| Berlin   | A    |    5 |   45 |   5 |   55 |  105 |
| Berlin   | B    |    7 |   45 |   7 |   59 |  111 |
| Budapest | B    |    5 |   45 |   5 |   55 |  105 |
| Budapest | B    |   11 |   45 |  11 |   67 |  123 |
| London   | A    |    8 |   45 |  24 |   77 |  146 |
| London   | A    |    9 |   45 |   9 |   63 |  117 |
+----------+------+------+------+-----+------+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            SUM('size', AS('asd')),
            ADD('wsum', SUM('size'), 'size', AS('add1')),
            ADD('wsum', SUM('size'), 'size', ADD(SUM('size'), SUM('size', OVER())), AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum,
            SUM(size) AS asd,
            SUM(size) OVER() + SUM(size) + size AS add1,
            SUM(size) OVER() + SUM(size) + size + (SUM(size) + SUM(size) OVER()) AS add2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING size > 3
        ORDER BY city`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });
});


describe('groupby with aggregates + win functions:', () => {
    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+------+-------+
| city     | name | size |  sum | wsum | wsum2 |
+----------+------+------+------+------+-------+
| Berlin   | A    |    5 |    5 |   62 |    81 |
| Berlin   | B    |    1 |    1 |   62 |    81 |
| Berlin   | B    |    2 |    2 |   62 |    81 |
| Berlin   | B    |    3 |    3 |   62 |    81 |
| Berlin   | B    |    7 |    7 |   62 |    81 |
| Berlin   | C    |    1 |    1 |   62 |    81 |
| Berlin   | C    |    2 |    2 |   62 |    81 |
| Berlin   | C    |    3 |    6 |   62 |    81 |
| Budapest | B    | null | null |   62 |    81 |
| Budapest | B    |    5 |    5 |   62 |    81 |
| Budapest | B    |   11 |   11 |   62 |    81 |
| Budapest | C    | null | null |   62 |    81 |
| Budapest | C    |    3 |    3 |   62 |    81 |
| London   | A    |    8 |   24 |   62 |    81 |
| London   | A    |    9 |    9 |   62 |    81 |
| London   | C    | null | null |   62 |    81 |
| London   | C    |    2 |    2 |   62 |    81 |
+----------+------+------+------+------+-------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            SUM(SUM('size'), OVER(), AS('wsum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) AS sum,
            SUM(size) OVER() AS wsum,
            SUM(SUM(size)) OVER() AS wsum2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
const table = `
+----------+------+------+------+------+------+------+
| city     | name | size |  sum | wsum | add1 | add2 |
+----------+------+------+------+------+------+------+
| Berlin   | A    |    5 |    5 |   62 |   72 |  139 |
| Berlin   | B    |    1 |    1 |   62 |   64 |  127 |
| Berlin   | B    |    2 |    2 |   62 |   66 |  130 |
| Berlin   | B    |    3 |    3 |   62 |   68 |  133 |
| Berlin   | B    |    7 |    7 |   62 |   76 |  145 |
| Berlin   | C    |    1 |    1 |   62 |   64 |  127 |
| Berlin   | C    |    2 |    2 |   62 |   66 |  130 |
| Berlin   | C    |    3 |    6 |   62 |   74 |  142 |
| Budapest | B    | null | null |   62 | null | null |
| Budapest | B    |    5 |    5 |   62 |   72 |  139 |
| Budapest | B    |   11 |   11 |   62 |   84 |  157 |
| Budapest | C    | null | null |   62 | null | null |
| Budapest | C    |    3 |    3 |   62 |   68 |  133 |
| London   | A    |    8 |   24 |   62 |  110 |  196 |
| London   | A    |    9 |    9 |   62 |   80 |  151 |
| London   | C    | null | null |   62 | null | null |
| London   | C    |    2 |    2 |   62 |   66 |  130 |
+----------+------+------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD(SUM('size'), 'sum', 'wsum', AS('add1')),
            ADD(SUM('size'), ADD(ADD(SUM('size')),ADD(ADD(SUM('size', OVER())))), 'sum', 'wsum', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) AS sum,
            SUM(size) OVER() AS wsum,
            SUM(size) + SUM(size) + SUM(size) OVER() AS add1,
            SUM(size) + (SUM(size) + SUM(size) OVER()) + SUM(size) + SUM(size) OVER() AS add2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });
});