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
+-----+------+
| sum | wsum |
+-----+------+
|  37 |   37 |
+-----+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`aggr + win + aw_scal`, () => {
        const table = `
+-----+------+-----+
| sum | wsum | add |
+-----+------+-----+
|  37 |   37 |  74 |
+-----+------+-----+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
            ADD('sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));
    });

    it(`aggr + win + aw_scal`, () => {
        const table = `
+------+-----+-----+
| wsum | add | sum |
+------+-----+-----+
|   37 |  74 |  37 |
+------+-----+-----+
1 row selected`;
        
        const res =
        SELECT(
            SUM('sum', OVER(), AS('wsum')),
            ADD('sum', 'wsum', AS('add')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`aggr + win + aw_scal`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('column size (SELECT 2) does not exist'));
    });

    it(`aggr + win + aw_scal`, () => {
        const query = () =>
        SELECT(
            // no bug, aggr-win comes after => fn is taken for scalar
            ADD('sum', 'wsum', AS('add')),
            SUM('sum', OVER(), AS('wsum')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('GROUP_BY required'));
    });

    it(`err -> where`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.sum > 3),
        ORDER_BY('sum', 'wsum'))

        expect(query).toThrow(new Error('column sum (WHERE) does not exist'));
    });

    it(`err -> where`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.wsum > 3),
        ORDER_BY('sum', 'wsum'))

        expect(query).toThrow(new Error('column wsum (WHERE) does not exist'));
    });

    it(`err -> orderby`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('size', 'wsum'))

        expect(query).toThrow(new Error('column size (ORDER_BY) does not exist'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('wsum')),
            SUM('wsum', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('nesting error (SELECT 2 wsum)'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum1')),
            SUM('sum1', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('nesting error (SELECT 2 sum1)'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('wsum1')),
            SUM('wsum1', OVER(), AS('wsum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('nesting error (SELECT 2 wsum1)'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            SUM(SUM('size', OVER()), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`nesting`, () => {
        const table = `
+------+
| wsum |
+------+
|   37 |
+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM(SUM('size'), OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(SUM(size)) OVER() AS wsum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY wsum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`nesting`, () => {
const table = `
+------+
| wsum |
+------+
|   74 |
+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM(ADD(SUM('size'), SUM('size')), OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(SUM(size) + SUM(size)) OVER() AS wsum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY wsum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            SUM(ADD(SUM('size', OVER()), SUM('size', OVER())), OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`aggr + win + aw_scal`, () => {
        const query = () =>
        SELECT(
            ADD(SUM('size'), SUM('size', OVER()), AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('add'))

        expect(query).toThrow(new Error('column size (SELECT 1) does not exist'));
    });

    it(`err -> orderby`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum')),
            SUM('sum', OVER(), AS('wsum')),
            ADD('sum', SUM('size', OVER()), AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('wsum'))

        expect(query).toThrow(new Error('column size (SELECT 3) does not exist'));
    });

});

describe('groupby (without aggregates) + win functions:', () => {
    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+
| city     | name | size | wsum |
+----------+------+------+------+
| Berlin   | A    |    5 |   45 |
| Berlin   | B    |    7 |   45 |
| Budapest | B    |    5 |   45 |
| Budapest | B    |   11 |   45 |
| London   | A    |    8 |   45 |
| London   | A    |    9 |   45 |
+----------+------+------+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum
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
+----------+------+------+------+-----+
| city     | name | size | wsum | add |
+----------+------+------+------+-----+
| Berlin   | A    |    5 |   45 |  50 |
| Berlin   | B    |    7 |   45 |  52 |
| Budapest | B    |    5 |   45 |  50 |
| Budapest | B    |   11 |   45 |  56 |
| London   | A    |    8 |   45 |  53 |
| London   | A    |    9 |   45 |  54 |
+----------+------+------+------+-----+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            ADD('wsum', 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum,
            SUM(size) OVER() + size AS 'add'
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
+----------+------+------+------+-----+
| city     | name | size | wsum | add |
+----------+------+------+------+-----+
| Berlin   | A    |    5 |   45 |  50 |
| Budapest | B    |    5 |   45 |  50 |
| Berlin   | B    |    7 |   45 |  52 |
| London   | A    |    8 |   45 |  53 |
| London   | A    |    9 |   45 |  54 |
| Budapest | B    |   11 |   45 |  56 |
+----------+------+------+------+-----+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            ADD('wsum', 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('size', 'add'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum,
            SUM(size) OVER() + size AS 'add'
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING size > 3
        ORDER BY size, 'add'`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('type', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column type (SELECT 4) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.wsum < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column wsum (WHERE) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.wsum > 3),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column wsum (HAVING) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'type'))

        expect(query).toThrow(new Error('column type (ORDER_BY) does not exist'));
    });

    it(`groupby 1`, () => {
const table = `
+----------+------+------+------+-----+------+
| city     | name | size | wsum | asd | add1 |
+----------+------+------+------+-----+------+
| Berlin   | A    |    5 |   45 |   5 |   55 |
| Berlin   | B    |    7 |   45 |   7 |   59 |
| Budapest | B    |    5 |   45 |   5 |   55 |
| Budapest | B    |   11 |   45 |  11 |   67 |
| London   | A    |    8 |   45 |  24 |   77 |
| London   | A    |    9 |   45 |   9 |   63 |
+----------+------+------+------+-----+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            SUM('size', AS('asd')),
            ADD('wsum', SUM('size'), 'size', AS('add1')),
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
            SUM(size) OVER() + SUM(size) + size AS add1
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING size > 3
        ORDER BY city`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+------+
| city     | name | size | wsum | add1 |
+----------+------+------+------+------+
| Berlin   | A    |    5 |   45 |   50 |
| Berlin   | B    |    7 |   45 |   52 |
| Budapest | B    |    5 |   45 |   50 |
| Budapest | B    |   11 |   45 |   56 |
| London   | A    |    8 |   45 |   69 |
| London   | A    |    9 |   45 |   54 |
+----------+------+------+------+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', OVER(), AS('wsum')),
            ADD('wsum', SUM('size'), AS('add1')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() AS wsum,
            SUM(size) OVER() + SUM(size) AS add1
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
+----------+------+------+------+------+
| city     | name | size |  sum | wsum |
+----------+------+------+------+------+
| Berlin   | A    |    5 |    5 |   62 |
| Berlin   | B    |    1 |    1 |   62 |
| Berlin   | B    |    2 |    2 |   62 |
| Berlin   | B    |    3 |    3 |   62 |
| Berlin   | B    |    7 |    7 |   62 |
| Berlin   | C    |    1 |    1 |   62 |
| Berlin   | C    |    2 |    2 |   62 |
| Berlin   | C    |    3 |    6 |   62 |
| Budapest | B    | null | null |   62 |
| Budapest | B    |    5 |    5 |   62 |
| Budapest | B    |   11 |   11 |   62 |
| Budapest | C    | null | null |   62 |
| Budapest | C    |    3 |    3 |   62 |
| London   | A    |    8 |   24 |   62 |
| London   | A    |    9 |    9 |   62 |
| London   | C    | null | null |   62 |
| London   | C    |    2 |    2 |   62 |
+----------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) AS sum,
            SUM(size) OVER() AS wsum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+------+------+
| city     | name | size |  sum | wsum |  add |
+----------+------+------+------+------+------+
| Berlin   | A    |    5 |    5 |   62 |   72 |
| Berlin   | B    |    1 |    1 |   62 |   64 |
| Berlin   | B    |    2 |    2 |   62 |   66 |
| Berlin   | B    |    3 |    3 |   62 |   68 |
| Berlin   | B    |    7 |    7 |   62 |   76 |
| Berlin   | C    |    1 |    1 |   62 |   64 |
| Berlin   | C    |    2 |    2 |   62 |   66 |
| Berlin   | C    |    3 |    6 |   62 |   71 |
| Budapest | B    | null | null |   62 | null |
| Budapest | B    |    5 |    5 |   62 |   72 |
| Budapest | B    |   11 |   11 |   62 |   84 |
| Budapest | C    | null | null |   62 | null |
| Budapest | C    |    3 |    3 |   62 |   68 |
| London   | A    |    8 |   24 |   62 |   94 |
| London   | A    |    9 |    9 |   62 |   80 |
| London   | C    | null | null |   62 | null |
| London   | C    |    2 |    2 |   62 |   66 |
+----------+------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('size', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('size', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.sum < 100),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column sum (WHERE) does not exist'));
    });

    it(`groupby 1`, () => {
const table = `
+----------+------+------+------+------+------+
| city     | name | size |  sum | wsum | add1 |
+----------+------+------+------+------+------+
| Berlin   | A    |    5 |    5 |   62 |   72 |
| Berlin   | B    |    1 |    1 |   62 |   64 |
| Berlin   | B    |    2 |    2 |   62 |   66 |
| Berlin   | B    |    3 |    3 |   62 |   68 |
| Berlin   | B    |    7 |    7 |   62 |   76 |
| Berlin   | C    |    1 |    1 |   62 |   64 |
| Berlin   | C    |    2 |    2 |   62 |   66 |
| Berlin   | C    |    3 |    6 |   62 |   74 |
| Budapest | B    | null | null |   62 | null |
| Budapest | B    |    5 |    5 |   62 |   72 |
| Budapest | B    |   11 |   11 |   62 |   84 |
| Budapest | C    | null | null |   62 | null |
| Budapest | C    |    3 |    3 |   62 |   68 |
| London   | A    |    8 |   24 |   62 |  110 |
| London   | A    |    9 |    9 |   62 |   80 |
| London   | C    | null | null |   62 | null |
| London   | C    |    2 |    2 |   62 |   66 |
+----------+------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD(SUM('size'), 'sum', 'wsum', AS('add1')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) AS sum,
            SUM(size) OVER() AS wsum,
            SUM(size) + SUM(size) + SUM(size) OVER() AS add1
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
        const table = `
+----------+------+------+------+
| city     | name | size | add1 |
+----------+------+------+------+
| Berlin   | A    |    5 |   67 |
| Berlin   | B    |    1 |   63 |
| Berlin   | B    |    2 |   64 |
| Berlin   | B    |    3 |   65 |
| Berlin   | B    |    7 |   69 |
| Berlin   | C    |    1 |   63 |
| Berlin   | C    |    2 |   64 |
| Berlin   | C    |    3 |   68 |
| Budapest | B    | null | null |
| Budapest | B    |    5 |   67 |
| Budapest | B    |   11 |   73 |
| Budapest | C    | null | null |
| Budapest | C    |    3 |   65 |
| London   | A    |    8 |   86 |
| London   | A    |    9 |   71 |
| London   | C    | null | null |
| London   | C    |    2 |   64 |
+----------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            ADD(SUM('size', OVER()), SUM('size'), AS('add1')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            SUM(size) OVER() + SUM(size) AS add1
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('size', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.sum < 100),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column sum (WHERE) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('type', AS('sum')),
            SUM('type', OVER(), AS('wsum')),
            ADD('size', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column type (SELECT 5) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('type', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('type', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column type (SELECT 6) does not exist'));
    });

    it(`groupby 1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('type', AS('sum')),
            SUM('size', OVER(), AS('wsum')),
            ADD('size', 'sum', 'wsum', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'type'))

        expect(query).toThrow(new Error('column type (ORDER_BY) does not exist'));
    });
});