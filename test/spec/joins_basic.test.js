import { SELECT, JOIN, FROM, ON, USING, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN,
         CROSS_JOIN, ORDER_BY } from '../../dist/select.js'
import { input1, input2, input20, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();


import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input1, 't1')
createTable(db, input2, 't2')
createTable(db, input20, 't20')



describe('joins_1 -> all joins -> 2 inputs:', () => {
    describe('not empty inputs:', () => {
        it(`inner join - with ON`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
+-------+------+-------+-----+
4 rows selected
`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id')))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 JOIN t2 ON(t1.id = t2.id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`inner join - with USING`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
4 rows selected
`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id')))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 JOIN t2 USING(id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`leftjoin - with ON`, () => {
            const table = `
+-------+------+-------+------+
| t1.id | name | t2.id |  age |
+-------+------+-------+------+
|     1 | A    |  null | null |
|     2 | B    |  null | null |
|     3 | C    |     3 |    1 |
|  null | D    |  null | null |
|     3 | E    |     3 |    1 |
|     4 | F    |     4 |   11 |
|     4 | F    |     4 |   22 |
|  null | G    |  null | null |
+-------+------+-------+------+
8 rows selected
`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_JOIN, input2, ON('id', 'id')))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 LEFT JOIN t2 ON(t1.id = t2.id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`leftjoin - with USING`, () => {
            const table = `
+------+------+------+
|   id | name |  age |
+------+------+------+
|    1 | A    | null |
|    2 | B    | null |
|    3 | C    |    1 |
| null | D    | null |
|    3 | E    |    1 |
|    4 | F    |   11 |
|    4 | F    |   22 |
| null | G    | null |
+------+------+------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_JOIN, input2, USING('id')))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 LEFT JOIN t2 USING(id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`rightjoin - with ON`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
|  null | null |  null |  33 |
|  null | null |     5 |  44 |
|  null | null |     6 |  55 |
|  null | null |  null |  66 |
+-------+------+-------+-----+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, input2, ON('id', 'id')))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t2 LEFT JOIN t1 ON(t1.id = t2.id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`rightjoin - with USING`, () => {
            const table = `
+------+------+-----+
|   id | name | age |
+------+------+-----+
|    3 | C    |   1 |
|    3 | E    |   1 |
|    4 | F    |  11 |
|    4 | F    |  22 |
| null | null |  33 |
|    5 | null |  44 |
|    6 | null |  55 |
| null | null |  66 |
+------+------+-----+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, input2, USING('id')))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t2 LEFT JOIN t1 USING(id)`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`fulljoin - with ON`, () => {
            const table = `
+-------+------+-------+------+
| t1.id | name | t2.id |  age |
+-------+------+-------+------+
|     1 | A    |  null | null |
|     2 | B    |  null | null |
|     3 | C    |     3 |    1 |
|  null | D    |  null | null |
|     3 | E    |     3 |    1 |
|     4 | F    |     4 |   11 |
|     4 | F    |     4 |   22 |
|  null | G    |  null | null |
|  null | null |  null |   33 |
|  null | null |     5 |   44 |
|  null | null |     6 |   55 |
|  null | null |  null |   66 |
+-------+------+-------+------+
12 rows selected
`;

            const res =
            SELECT('*',
            FROM(input1, FULL_JOIN, input2, ON('id', 'id')))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 LEFT JOIN t2 ON(t1.id = t2.id)
            UNION ALL
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t2 LEFT JOIN t1 ON(t1.id = t2.id)
            WHERE t1.id IS NULL`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`fulljoin - with USING`, () => {
            const table = `
+------+------+------+
|   id | name |  age |
+------+------+------+
|    1 | A    | null |
|    2 | B    | null |
|    3 | C    |    1 |
| null | D    | null |
|    3 | E    |    1 |
|    4 | F    |   11 |
|    4 | F    |   22 |
| null | G    | null |
| null | null |   33 |
|    5 | null |   44 |
|    6 | null |   55 |
| null | null |   66 |
+------+------+------+
12 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, FULL_JOIN, input2, USING('id')))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 LEFT JOIN t2 USING(id)
            UNION ALL
            SELECT id, name, age
            FROM t2 LEFT JOIN t1 USING(id)
            WHERE t1.id IS NULL`).all();
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

////////////////////////////////////

        it(`inner join - with ON`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
|     4 | F    |     4 |  25 |
+-------+------+-------+-----+
5 rows selected
`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input20, ON('id', 'id')))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 JOIN t20 t2 ON(t1.id = t2.id)`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`inner join - with USING`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
|  4 | F    |  25 |
+----+------+-----+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input20, USING('id')))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 JOIN t20 t2 USING(id)`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`leftjoin - with ON`, () => {
            const table = `
+-------+------+-------+------+
| t1.id | name | t2.id |  age |
+-------+------+-------+------+
|  null | D    |  null | null |
|  null | G    |  null | null |
|     1 | A    |  null | null |
|     2 | B    |  null | null |
|     3 | C    |     3 |    1 |
|     3 | E    |     3 |    1 |
|     4 | F    |     4 |   11 |
|     4 | F    |     4 |   22 |
|     4 | F    |     4 |   25 |
+-------+------+-------+------+
9 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_JOIN, input20, ON('id', 'id')),
            ORDER_BY('t1.id'))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 LEFT JOIN t20 t2 ON(t1.id = t2.id)
            ORDER BY t1.id`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`leftjoin - with USING`, () => {
            const table = `
+------+------+------+
|   id | name |  age |
+------+------+------+
| null | D    | null |
| null | G    | null |
|    1 | A    | null |
|    2 | B    | null |
|    3 | C    |    1 |
|    3 | E    |    1 |
|    4 | F    |   11 |
|    4 | F    |   22 |
|    4 | F    |   25 |
+------+------+------+
9 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_JOIN, input20, USING('id')),
            ORDER_BY('id'))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 LEFT JOIN t20 t2 USING(id)
            ORDER BY id`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`rightjoin - with ON`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|  null | null |  null |  33 |
|  null | null |     5 |  44 |
|  null | null |     6 |  55 |
|  null | null |     7 |  60 |
|  null | null |  null |  66 |
|  null | null |  null |  50 |
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
|     4 | F    |     4 |  25 |
+-------+------+-------+-----+
11 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, input20, ON('id', 'id')),
            ORDER_BY('t1.id'))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t20 t2 LEFT JOIN t1 ON(t1.id = t2.id)
            ORDER BY t1.id`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`rightjoin - with USING`, () => {
            const table = `
+------+------+-----+
|   id | name | age |
+------+------+-----+
| null | null |  33 |
| null | null |  66 |
| null | null |  50 |
|    3 | C    |   1 |
|    3 | E    |   1 |
|    4 | F    |  11 |
|    4 | F    |  22 |
|    4 | F    |  25 |
|    5 | null |  44 |
|    6 | null |  55 |
|    7 | null |  60 |
+------+------+-----+
11 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, input20, USING('id')),
            ORDER_BY('id'))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t20 t2 LEFT JOIN t1 USING(id)
            ORDER BY id`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`fulljoin - with ON`, () => {
            const table = `
+-------+------+-------+------+
| t1.id | name | t2.id |  age |
+-------+------+-------+------+
|  null | null |  null |   33 |
|  null | null |     5 |   44 |
|  null | null |     6 |   55 |
|  null | null |     7 |   60 |
|  null | null |  null |   66 |
|  null | null |  null |   50 |
|  null | D    |  null | null |
|  null | G    |  null | null |
|     1 | A    |  null | null |
|     2 | B    |  null | null |
|     3 | C    |     3 |    1 |
|     3 | E    |     3 |    1 |
|     4 | F    |     4 |   11 |
|     4 | F    |     4 |   22 |
|     4 | F    |     4 |   25 |
+-------+------+-------+------+
15 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, FULL_JOIN, input20, ON('id', 'id')),
            ORDER_BY('t1.id', 'name'))

            const expected =
            db.prepare(`
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t1 LEFT JOIN t20 t2 ON(t1.id = t2.id)
            UNION ALL
            SELECT t1.id AS 't1.id', name, t2.id AS 't2.id', age
            FROM t20 t2 LEFT JOIN t1 ON(t1.id = t2.id)
            WHERE t1.id IS NULL
            ORDER BY t1.id, name`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });

        it(`fulljoin - with USING`, () => {
            const table = `
+------+------+------+
|   id | name |  age |
+------+------+------+
| null | null |   33 |
| null | null |   66 |
| null | null |   50 |
| null | D    | null |
| null | G    | null |
|    1 | A    | null |
|    2 | B    | null |
|    3 | C    |    1 |
|    3 | E    |    1 |
|    4 | F    |   11 |
|    4 | F    |   22 |
|    4 | F    |   25 |
|    5 | null |   44 |
|    6 | null |   55 |
|    7 | null |   60 |
+------+------+------+
15 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, FULL_JOIN, input20, USING('id')),
            ORDER_BY('id', 'name'))

            const expected =
            db.prepare(`
            SELECT id, name, age
            FROM t1 LEFT JOIN t20 t2 USING(id)
            UNION ALL
            SELECT id, name, age
            FROM t20 t2 LEFT JOIN t1 USING(id)
            WHERE t1.id IS NULL
            ORDER BY id, name`).all();

            expect(trim(drawTable(res))).toEqual(trim(table));
            expect(res).toEqual(expected);
        });


        it(`crossjoin`, () => {
            const input1 = [
                { id: 1, name: 'A' },
                { id: 2, name: 'B' }
            ]
    
            const input2 = [
                { id: 3, age: 1 },
                { id: 4, age: 11 }
            ]

            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     1 | A    |     3 |   1 |
|     1 | A    |     4 |  11 |
|     2 | B    |     3 |   1 |
|     2 | B    |     4 |  11 |
+-------+------+-------+-----+
4 rows selected
`;

            const res1 =
            SELECT('*',
            FROM(input1, CROSS_JOIN, input2))

            const res2 =
            SELECT('*',
            FROM(input1, input2))

            expect(trim(drawTable(res1))).toEqual(trim(table));
            expect(trim(drawTable(res2))).toEqual(trim(table));
        });

    });

    describe('empty inputs:', () => {
        it(`inner join - with ON`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, JOIN, [], ON('id', 'id')))

            const res2 =
            SELECT('*',
            FROM([], JOIN, input2, ON('id', 'id')))

            const res3 =
            SELECT('*',
            FROM([], JOIN, [], ON('id', 'id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`inner join - with USING`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, JOIN, [], USING('id')))

            const res2 =
            SELECT('*',
            FROM([], JOIN, input2, USING('id')))

            const res3 =
            SELECT('*',
            FROM([], JOIN, [], USING('id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`leftjoin - with ON`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, LEFT_JOIN, [], ON('id', 'id')))

            const res2 =
            SELECT('*',
            FROM([], LEFT_JOIN, input2, ON('id', 'id')))

            const res3 =
            SELECT('*',
            FROM([], LEFT_JOIN, [], ON('id', 'id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`leftjoin - with USING`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, LEFT_JOIN, [], USING('id')))

            const res2 =
            SELECT('*',
            FROM([], LEFT_JOIN, input2, USING('id')))

            const res3 =
            SELECT('*',
            FROM([], LEFT_JOIN, [], USING('id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`rightjoin - with ON`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, [], ON('id', 'id')))

            const res2 =
            SELECT('*',
            FROM([], RIGHT_JOIN, input2, ON('id', 'id')))

            const res3 =
            SELECT('*',
            FROM([], RIGHT_JOIN, [], ON('id', 'id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`rightjoin - with USING`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, RIGHT_JOIN, [], USING('id')))

            const res2 =
            SELECT('*',
            FROM([], RIGHT_JOIN, input2, USING('id')))

            const res3 =
            SELECT('*',
            FROM([], RIGHT_JOIN, [], USING('id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`fulljoin - with ON`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, FULL_JOIN, [], ON('id', 'id')))

            const res2 =
            SELECT('*',
            FROM([], FULL_JOIN, input2, ON('id', 'id')))

            const res3 =
            SELECT('*',
            FROM([], FULL_JOIN, [], ON('id', 'id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });

        it(`fulljoin - with USING`, () => {
            const res1 =
            SELECT('*',
            FROM(input1, FULL_JOIN, [], USING('id')))

            const res2 =
            SELECT('*',
            FROM([], FULL_JOIN, input2, USING('id')))

            const res3 =
            SELECT('*',
            FROM([], FULL_JOIN, [], USING('id')))

            expect(res1).toEqual([]);
            expect(res2).toEqual([]);
            expect(res3).toEqual([]);
        });
    });

    it(`crossjoin`, () => {
        const input1 = [
            { id: 1, name: 'A' },
            { id: 2, name: 'B' }
        ]

        const input2 = [
            { id: 3, age: 1 },
            { id: 4, age: 11 }
        ]

        const res1 =
        SELECT('*',
        FROM(input1, CROSS_JOIN, []))

        const res2 =
        SELECT('*',
        FROM([], input2))

        expect(res1).toEqual([]);
        expect(res2).toEqual([]);
    });
});