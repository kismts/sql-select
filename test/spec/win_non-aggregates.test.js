import { SELECT, FROM, JOIN, ON, ORDER_BY, AS, OVER, SUM, GROUP_BY,
    PARTITION_BY, ROW_NUMBER, RANK,
    DENSE_RANK, PERCENT_RANK, CUME_DIST, LEAD, LAG, NTILE, FIRST_VALUE, LAST_VALUE, NTH_VALUE,
    PERCENTILE_CONT, PERCENTILE_DISC, MEDIAN, WITHIN_GROUP } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')



describe('non-aggregate window functions:', () => {
    it(`ranking functions`, () => {
        const table = `
+----------+------+---------+------+------------+
| city     | size | row_num | rank | dense_rank |
+----------+------+---------+------+------------+
| Berlin   |    1 |       1 |    1 |          1 |
| Berlin   |    1 |       2 |    1 |          1 |
| Berlin   |    2 |       3 |    3 |          2 |
| Berlin   |    2 |       4 |    3 |          2 |
| Berlin   |    3 |       5 |    5 |          3 |
| Berlin   |    3 |       6 |    5 |          3 |
| Berlin   |    3 |       7 |    5 |          3 |
| Berlin   |    5 |       8 |    8 |          4 |
| Berlin   |    7 |       9 |    9 |          5 |
| Budapest | null |       1 |    1 |          1 |
| Budapest | null |       2 |    1 |          1 |
| Budapest | null |       3 |    1 |          1 |
| Budapest | null |       4 |    1 |          1 |
| Budapest |    3 |       5 |    5 |          2 |
| Budapest |    5 |       6 |    6 |          3 |
| Budapest |   11 |       7 |    7 |          4 |
| London   | null |       1 |    1 |          1 |
| London   |    2 |       2 |    2 |          2 |
| London   |    8 |       3 |    3 |          3 |
| London   |    8 |       4 |    3 |          3 |
| London   |    8 |       5 |    3 |          3 |
| London   |    9 |       6 |    6 |          4 |
| Madrid   |    2 |       1 |    1 |          1 |
| Madrid   |    3 |       2 |    2 |          2 |
| Madrid   |   22 |       3 |    3 |          3 |
| Paris    | null |       1 |    1 |          1 |
| Paris    |    4 |       2 |    2 |          2 |
| Paris    |    4 |       3 |    2 |          2 |
| Paris    |    4 |       4 |    2 |          2 |
| Paris    |    7 |       5 |    5 |          3 |
| Rome     | null |       1 |    1 |          1 |
| Rome     | null |       2 |    1 |          1 |
| Rome     |    2 |       3 |    3 |          2 |
| Rome     |    4 |       4 |    4 |          3 |
| Rome     |   10 |       5 |    5 |          4 |
| Rome     |   11 |       6 |    6 |          5 |
+----------+------+---------+------+------------+
36 rows selected`;

        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
        
        const res =
        SELECT('city', 'size',
            ROW_NUMBER(win, AS('row_num')),
            RANK(win, AS('rank')),
            DENSE_RANK(win, AS('dense_rank')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _win = `OVER(PARTITION BY city ORDER BY size)`

        const expected =
        db.prepare(`
        SELECT city, size,
            ROW_NUMBER() ${ _win } AS row_num,
            RANK() ${ _win } AS rank,
            DENSE_RANK() ${ _win } AS dense_rank
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`distribution functions`, () => {
        const table = `
+----------+------+--------------------+---------------------+
| city     | size |          perc_rank |           cume_dist |
+----------+------+--------------------+---------------------+
| Berlin   |    1 |                  0 |  0.2222222222222222 |
| Berlin   |    1 |                  0 |  0.2222222222222222 |
| Berlin   |    2 |               0.25 |  0.4444444444444444 |
| Berlin   |    2 |               0.25 |  0.4444444444444444 |
| Berlin   |    3 |                0.5 |  0.7777777777777778 |
| Berlin   |    3 |                0.5 |  0.7777777777777778 |
| Berlin   |    3 |                0.5 |  0.7777777777777778 |
| Berlin   |    5 |              0.875 |  0.8888888888888888 |
| Berlin   |    7 |                  1 |                   1 |
| Budapest | null |                  0 |  0.5714285714285714 |
| Budapest | null |                  0 |  0.5714285714285714 |
| Budapest | null |                  0 |  0.5714285714285714 |
| Budapest | null |                  0 |  0.5714285714285714 |
| Budapest |    3 | 0.6666666666666666 |  0.7142857142857143 |
| Budapest |    5 | 0.8333333333333334 |  0.8571428571428571 |
| Budapest |   11 |                  1 |                   1 |
| London   | null |                  0 | 0.16666666666666666 |
| London   |    2 |                0.2 |  0.3333333333333333 |
| London   |    8 |                0.4 |  0.8333333333333334 |
| London   |    8 |                0.4 |  0.8333333333333334 |
| London   |    8 |                0.4 |  0.8333333333333334 |
| London   |    9 |                  1 |                   1 |
| Madrid   |    2 |                  0 |  0.3333333333333333 |
| Madrid   |    3 |                0.5 |  0.6666666666666666 |
| Madrid   |   22 |                  1 |                   1 |
| Paris    | null |                  0 |                 0.2 |
| Paris    |    4 |               0.25 |                 0.8 |
| Paris    |    4 |               0.25 |                 0.8 |
| Paris    |    4 |               0.25 |                 0.8 |
| Paris    |    7 |                  1 |                   1 |
| Rome     | null |                  0 |  0.3333333333333333 |
| Rome     | null |                  0 |  0.3333333333333333 |
| Rome     |    2 |                0.4 |                 0.5 |
| Rome     |    4 |                0.6 |  0.6666666666666666 |
| Rome     |   10 |                0.8 |  0.8333333333333334 |
| Rome     |   11 |                  1 |                   1 |
+----------+------+--------------------+---------------------+
36 rows selected`;

        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
        
        const res =
        SELECT('city', 'size',
            PERCENT_RANK(win, AS('perc_rank')),
            CUME_DIST(win, AS('cume_dist')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _win = `OVER(PARTITION BY city ORDER BY size)`

        const expected =
        db.prepare(`
        SELECT city, size,
            PERCENT_RANK() ${ _win } AS perc_rank,
            CUME_DIST() ${ _win } AS cume_dist
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`analytic functions -> without frames`, () => {
        const table = `
+----------+------+------+--------+-------------+----------+------+-------+------------+---------+-------+
| city     | size | lead | lead_2 | lead_2_null | lead_2_0 |  lag | lag_2 | lag_2_null | lag_2_0 | ntile |
+----------+------+------+--------+-------------+----------+------+-------+------------+---------+-------+
| Berlin   |    1 |    1 |      2 |           2 |        2 | null |  null |       null |       0 |     1 |
| Berlin   |    1 |    2 |      2 |           2 |        2 |    1 |  null |       null |       0 |     1 |
| Berlin   |    2 |    2 |      3 |           3 |        3 |    1 |     1 |          1 |       1 |     1 |
| Berlin   |    2 |    3 |      3 |           3 |        3 |    2 |     1 |          1 |       1 |     2 |
| Berlin   |    3 |    3 |      3 |           3 |        3 |    2 |     2 |          2 |       2 |     2 |
| Berlin   |    3 |    3 |      5 |           5 |        5 |    3 |     2 |          2 |       2 |     2 |
| Berlin   |    3 |    5 |      7 |           7 |        7 |    3 |     3 |          3 |       3 |     3 |
| Berlin   |    5 |    7 |   null |        null |        0 |    3 |     3 |          3 |       3 |     3 |
| Berlin   |    7 | null |   null |        null |        0 |    5 |     3 |          3 |       3 |     3 |
| Budapest | null | null |   null |        null |     null | null |  null |       null |       0 |     1 |
| Budapest | null | null |   null |        null |     null | null |  null |       null |       0 |     1 |
| Budapest | null | null |      3 |           3 |        3 | null |  null |       null |    null |     1 |
| Budapest | null |    3 |      5 |           5 |        5 | null |  null |       null |    null |     2 |
| Budapest |    3 |    5 |     11 |          11 |       11 | null |  null |       null |    null |     2 |
| Budapest |    5 |   11 |   null |        null |        0 |    3 |  null |       null |    null |     3 |
| Budapest |   11 | null |   null |        null |        0 |    5 |     3 |          3 |       3 |     3 |
| London   | null |    2 |      8 |           8 |        8 | null |  null |       null |       0 |     1 |
| London   |    2 |    8 |      8 |           8 |        8 | null |  null |       null |       0 |     1 |
| London   |    8 |    8 |      8 |           8 |        8 |    2 |  null |       null |    null |     2 |
| London   |    8 |    8 |      9 |           9 |        9 |    8 |     2 |          2 |       2 |     2 |
| London   |    8 |    9 |   null |        null |        0 |    8 |     8 |          8 |       8 |     3 |
| London   |    9 | null |   null |        null |        0 |    8 |     8 |          8 |       8 |     3 |
| Madrid   |    2 |    3 |     22 |          22 |       22 | null |  null |       null |       0 |     1 |
| Madrid   |    3 |   22 |   null |        null |        0 |    2 |  null |       null |       0 |     2 |
| Madrid   |   22 | null |   null |        null |        0 |    3 |     2 |          2 |       2 |     3 |
| Paris    | null |    4 |      4 |           4 |        4 | null |  null |       null |       0 |     1 |
| Paris    |    4 |    4 |      4 |           4 |        4 | null |  null |       null |       0 |     1 |
| Paris    |    4 |    4 |      7 |           7 |        7 |    4 |  null |       null |    null |     2 |
| Paris    |    4 |    7 |   null |        null |        0 |    4 |     4 |          4 |       4 |     2 |
| Paris    |    7 | null |   null |        null |        0 |    4 |     4 |          4 |       4 |     3 |
| Rome     | null | null |      2 |           2 |        2 | null |  null |       null |       0 |     1 |
| Rome     | null |    2 |      4 |           4 |        4 | null |  null |       null |       0 |     1 |
| Rome     |    2 |    4 |     10 |          10 |       10 | null |  null |       null |    null |     2 |
| Rome     |    4 |   10 |     11 |          11 |       11 |    2 |  null |       null |    null |     2 |
| Rome     |   10 |   11 |   null |        null |        0 |    4 |     2 |          2 |       2 |     3 |
| Rome     |   11 | null |   null |        null |        0 |   10 |     4 |          4 |       4 |     3 |
+----------+------+------+--------+-------------+----------+------+-------+------------+---------+-------+
36 rows selected`;

        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
        
        const res =
        SELECT('city', 'size',
            LEAD('size', win, AS('lead')),
            LEAD('size', 2, win, AS('lead_2')),
            LEAD('size', 2, null, win, AS('lead_2_null')),
            LEAD('size', 2, 0, win, AS('lead_2_0')),
            LAG('size', win, AS('lag')),
            LAG('size', 2, win, AS('lag_2')),
            LAG('size', 2, null, win, AS('lag_2_null')),
            LAG('size', 2, 0, win, AS('lag_2_0')),
            NTILE(3, win, AS('ntile')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _win = `OVER(PARTITION BY city ORDER BY size)`

        const expected =
        db.prepare(`
        SELECT city, size,
            LEAD(size) ${ _win } AS lead,
            LEAD(size, 2) ${ _win } AS lead_2,
            LEAD(size, 2, null) ${ _win } AS lead_2_null,
            LEAD(size, 2, 0) ${ _win } AS lead_2_0,
            LAG(size) ${ _win } AS lag,
            LAG(size, 2) ${ _win } AS lag_2,
            LAG(size, 2, null) ${ _win } AS lag_2_null,
            LAG(size, 2, 0) ${ _win } AS lag_2_0,
            NTILE(3) ${ _win } AS ntile
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`analytic functions -> with frames`, () => {
        const table = `
+----------+------+-------------+------------+-----------+
| city     | size | first_value | last_value | nth_value |
+----------+------+-------------+------------+-----------+
| Berlin   |    1 |           1 |          7 |         2 |
| Berlin   |    1 |           1 |          7 |         2 |
| Berlin   |    2 |           1 |          7 |         2 |
| Berlin   |    2 |           1 |          7 |         2 |
| Berlin   |    3 |           1 |          7 |         2 |
| Berlin   |    3 |           1 |          7 |         2 |
| Berlin   |    3 |           1 |          7 |         2 |
| Berlin   |    5 |           1 |          7 |         2 |
| Berlin   |    7 |           1 |          7 |         2 |
| Budapest | null |        null |         11 |      null |
| Budapest | null |        null |         11 |      null |
| Budapest | null |        null |         11 |      null |
| Budapest | null |        null |         11 |      null |
| Budapest |    3 |        null |         11 |      null |
| Budapest |    5 |        null |         11 |      null |
| Budapest |   11 |        null |         11 |      null |
| London   | null |        null |          9 |         8 |
| London   |    2 |        null |          9 |         8 |
| London   |    8 |        null |          9 |         8 |
| London   |    8 |        null |          9 |         8 |
| London   |    8 |        null |          9 |         8 |
| London   |    9 |        null |          9 |         8 |
| Madrid   |    2 |           2 |         22 |        22 |
| Madrid   |    3 |           2 |         22 |        22 |
| Madrid   |   22 |           2 |         22 |        22 |
| Paris    | null |        null |          7 |         4 |
| Paris    |    4 |        null |          7 |         4 |
| Paris    |    4 |        null |          7 |         4 |
| Paris    |    4 |        null |          7 |         4 |
| Paris    |    7 |        null |          7 |         4 |
| Rome     | null |        null |         11 |         2 |
| Rome     | null |        null |         11 |         2 |
| Rome     |    2 |        null |         11 |         2 |
| Rome     |    4 |        null |         11 |         2 |
| Rome     |   10 |        null |         11 |         2 |
| Rome     |   11 |        null |         11 |         2 |
+----------+------+-------------+------------+-----------+
36 rows selected`;

        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
        
        const res =
        SELECT('city', 'size',
            FIRST_VALUE('size', win, AS('first_value')),
            LAST_VALUE('size', win, AS('last_value')),
            NTH_VALUE('size', 3, win, AS('nth_value')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _win = `OVER(PARTITION BY city ORDER BY size)`
        const _win2 = `OVER(PARTITION BY city ORDER BY size
                            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`


        const expected =
        db.prepare(`
        SELECT city, size,
                FIRST_VALUE(size) ${ _win } AS first_value,
                LAST_VALUE(size) ${ _win2 } AS last_value,
                NTH_VALUE(size, 3) ${ _win2 } AS nth_value
            FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`percentile window functions`, () => {
        const expected = `
+----------+------+-----------+-----------+--------+
| city     | size | perc_cont | perc_disc | median |
+----------+------+-----------+-----------+--------+
| Berlin   |    1 |         3 |         3 |      3 |
| Berlin   |    1 |         3 |         3 |      3 |
| Berlin   |    2 |         3 |         3 |      3 |
| Berlin   |    2 |         3 |         3 |      3 |
| Berlin   |    3 |         3 |         3 |      3 |
| Berlin   |    3 |         3 |         3 |      3 |
| Berlin   |    3 |         3 |         3 |      3 |
| Berlin   |    5 |         3 |         3 |      3 |
| Berlin   |    7 |         3 |         3 |      3 |
| Budapest | null |         5 |         5 |      5 |
| Budapest | null |         5 |         5 |      5 |
| Budapest | null |         5 |         5 |      5 |
| Budapest | null |         5 |         5 |      5 |
| Budapest |    3 |         5 |         5 |      5 |
| Budapest |    5 |         5 |         5 |      5 |
| Budapest |   11 |         5 |         5 |      5 |
| London   | null |         8 |         8 |      8 |
| London   |    2 |         8 |         8 |      8 |
| London   |    8 |         8 |         8 |      8 |
| London   |    8 |         8 |         8 |      8 |
| London   |    8 |         8 |         8 |      8 |
| London   |    9 |         8 |         8 |      8 |
| Madrid   |    2 |         3 |         3 |      3 |
| Madrid   |    3 |         3 |         3 |      3 |
| Madrid   |   22 |         3 |         3 |      3 |
| Paris    | null |         4 |         4 |      4 |
| Paris    |    4 |         4 |         4 |      4 |
| Paris    |    4 |         4 |         4 |      4 |
| Paris    |    4 |         4 |         4 |      4 |
| Paris    |    7 |         4 |         4 |      4 |
| Rome     | null |         7 |         4 |      7 |
| Rome     | null |         7 |         4 |      7 |
| Rome     |    2 |         7 |         4 |      7 |
| Rome     |    4 |         7 |         4 |      7 |
| Rome     |   10 |         7 |         4 |      7 |
| Rome     |   11 |         7 |         4 |      7 |
+----------+------+-----------+-----------+--------+
36 rows selected`;

        const res =
        SELECT('city', 'size',
            PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')), OVER(PARTITION_BY('city')), AS('perc_cont')),
            PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('size')), OVER(PARTITION_BY('city')), AS('perc_disc')),
            MEDIAN('size', OVER(PARTITION_BY('city')), AS('median')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(expected));
    });

    it(`percentile aggregate functions`, () => {
        const expected = `
+----------+-----------+-----------+--------+
| city     | perc_cont | perc_disc | median |
+----------+-----------+-----------+--------+
| Berlin   |         3 |         3 |      3 |
| Budapest |         5 |         5 |      5 |
| London   |         8 |         8 |      8 |
| Madrid   |         3 |         3 |      3 |
| Paris    |         4 |         4 |      4 |
| Rome     |         7 |         4 |      7 |
+----------+-----------+-----------+--------+
6 rows selected`;

        const res =
        SELECT('city',
            PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
            PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_disc')),
            MEDIAN('size', AS('median')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'))

        expect(trim(drawTable(res))).toEqual(trim(expected));
    });

    describe('ranking functions:', () => {
        it(`no AS`, () => {        
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });
    
    
        it(`no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(AS('row_num')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
    
        it(`+arg`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
            
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(win, 1, AS('row_num')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });
    
        it(`+arg`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
            
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(1, win, AS('row_num')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });
    
        it(`no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(1, AS('row_num')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
    
        it(`no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                ROW_NUMBER(1, 1, AS('row_num')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
    
    });

    describe('analytic functions:', () => {
        it(`lead no AS`, () => {        
            const query = () =>
            SELECT('city', 'size',
                LEAD(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });
    
        it(`lead no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                LEAD(AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
    
        it(`lead no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                LEAD(1, AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
        
        it(`lead +arg`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
           
            const query = () =>
            SELECT('city', 'size',
                LEAD('size', win, 1, AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`lead wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
           
            const query = () =>
            SELECT('city', 'size',
                LEAD(1, win, AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('invalid parameter (SELECT 3)'));
        });

        it(`lead no OVER`, () => {        
            const query = () =>
            SELECT('city', 'size',
                LEAD('size', AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });
    
        it(`lead no int`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                LEAD('size', 1.2, win, AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 3)'));
        });
        
        it(`lead no int (2nd param)`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                LEAD('size', 'asd', null, win, AS('lead')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 3)'));
        });

        it(`ntile no params`, () => {    
            const query = () =>
            SELECT('city', 'size',
                NTILE(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });

        it(`ntile no OVER`, () => {    
            const query = () =>
            SELECT('city', 'size',
                NTILE(AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });

        it(`ntile less args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE(win, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });
    
        it(`ntile wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE(-3, win, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`ntile wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE(0, win, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`ntile wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE(2.3, win, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`ntile wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE({}, win, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`ntile no OVER`, () => {
            const query = () =>
            SELECT('city', 'size',
                NTILE(2, {}, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });

        it(`ntile +arg`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTILE(2, win, 1, AS('ntile')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`first_value no params`, () => {    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });

        it(`first_value no OVER`, () => {    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE(AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });

        it(`first_value no OVER`, () => {    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE('size', AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });

        it(`first_value no prop name`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE(win, AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });

        it(`first_value wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE(1, win, AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('invalid parameter (SELECT 3)'));
        });

        it(`first_value +args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE('size', 1, win, AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`first_value +args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                FIRST_VALUE('size', win, 1, AS('fist_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`nth_value no params`, () => {    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });

        it(`nth_value no OVER`, () => {    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE(AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('OVER expected (SELECT 3)'));
        });

        it(`nth_value less args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });

        it(`nth_value wrong first param`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE(1, 1, win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('invalid parameter (SELECT 3)'));
        });

        it(`nth_value wrong args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', {}, win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`nth_value wrong args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', 'asd', win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });
    
        it(`nth_value wrong args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', 1.2, win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('integer (> 0) expected (SELECT 3)'));
        });

        it(`nth_value +args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', 1, 1, win, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`nth_value +args`, () => {
            const win = OVER(PARTITION_BY('city'), ORDER_BY('size'))
    
            const query = () =>
            SELECT('city', 'size',
                NTH_VALUE('size', 1, win, 1, AS('nth_value')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });
    });

    describe('percentile window functions:', () => {
        it(`no params`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });

        it(`no wg`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('WITHIN_GROUP expected (SELECT 3)'));
        });

        it(`no wg`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(0.5, OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('WITHIN_GROUP expected (SELECT 3)'));
        });

        it(`no n param`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(WITHIN_GROUP(ORDER_BY('size')), OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });

        it(`no n param`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(OVER(PARTITION_BY('city')), WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });
    
        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(NaN, WITHIN_GROUP(ORDER_BY('size')), OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('number (0 <= n <= 1) expected (SELECT 3)'));
        });
    
        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(2, WITHIN_GROUP(ORDER_BY('size')), OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('number (0 <= n <= 1) expected (SELECT 3)'));
        });

        it(`+arg`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')),1, OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`WG -> no params`, () => {
            const query = () =>
            SELECT('city', 'size',
                PERCENTILE_CONT(0.5, WITHIN_GROUP(), OVER(PARTITION_BY('city')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('ORDER_BY expected (WITHIN_GROUP SELECT 3)'));
        });

        it(`no params`, () => {
            const query = () =>
            SELECT('city', 'size',
                MEDIAN(),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('AS expected (SELECT 3)'));
        });

        it(`no prop`, () => {
            const query = () =>
            SELECT('city', 'size',
                MEDIAN(OVER(PARTITION_BY('city')), AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('more args expected (SELECT 3)'));
        });

        it(`+arg`, () => {
            const query = () =>
            SELECT('city', 'size',
                MEDIAN('size', 1, OVER(PARTITION_BY('city')), AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('too many args (SELECT 3)'));
        });

        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city', 'size',
                MEDIAN(1, OVER(PARTITION_BY('city')), AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')))
        
            expect(query).toThrow(new Error('invalid parameters (SELECT 3)'));
        });

    });

    describe('percentile aggregate functions:', () => {
        it(`no params`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('AS expected (SELECT 2)'));
        });

        it(`no WG`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('WITHIN_GROUP expected (SELECT 2)'));
        });

        it(`no n`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('more args expected (SELECT 2)'));
        });
    
        it(`wrong n`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(NaN, WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('number (0 <= n <= 1) expected (SELECT 2)'));
        });
    
        it(`wrong n`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(2, WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('number (0 <= n <= 1) expected (SELECT 2)'));
        });
    
        it(`no WG`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(0.5, AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('WITHIN_GROUP expected (SELECT 2)'));
        });
    
        it(`no params in WG`, () => {
            const query = () =>
            SELECT('city',
                PERCENTILE_CONT(0.5, WITHIN_GROUP(), AS('perc_cont')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('ORDER_BY expected (WITHIN_GROUP SELECT 2)'));
        });

        it(`no params`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN(),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('AS expected (SELECT 2)'));
        });

        it(`no prop`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN(AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('more args expected (SELECT 2)'));
        });

        it(`+arg`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN('size', 1, AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('too many args (SELECT 2)'));
        });

        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN(1, AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('invalid parameters (SELECT 2)'));
        });

        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN(SUM('age'), AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('invalid parameters (SELECT 2)'));
        });

        it(`wrong first param`, () => {
            const query = () =>
            SELECT('city',
                MEDIAN('cityxxx', AS('median')),
            FROM(input4, JOIN, input5, ON('id', 'id')),
            GROUP_BY('city'))
        
            expect(query).toThrow(new Error('column cityxxx (SELECT 2) does not exist'));
        });



    });

});
