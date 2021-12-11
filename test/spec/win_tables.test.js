import fs from 'fs'
import { SELECT, FROM, ORDER_BY, DESC,AS, COUNT, SUM, AVG, MAX, MIN,
         OVER, PARTITION_BY, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING,
         ROWS_BETWEEN, CURRENT_ROW, FOLLOWING, PRECEDING, RANGE_BETWEEN, GROUPS_BETWEEN, ASC,
         ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST,
         LEAD, LAG, NTILE, FIRST_VALUE, LAST_VALUE, NTH_VALUE } from '../../dist/select.js'

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');

import { drawTable } from './utils.js'
import { createTable } from './data.js'
import { randoms, randomsFloat, sort, random, len, each } from './help.js'

const Tables = {
    Tracks: 'tracks',
    Invoices2: 'invoices2',
    TracksRand: 'tracksrand'
};

const test_table = Tables.Invoices2;

let part_column, ord_column, column, table, _table, num_of_tests, gen_nulls, gen_rands, num_rands;
let max_frame_boundery_rows, max_frame_boundery_range, num_nulls_ord_columns, num_nulls_columns;
let log = false
let logTable = false


switch (test_table) {
    case Tables.Tracks:
        table = JSON.parse(fs.readFileSync('./spec/chinook-db/Track.json'));
        _table = Tables.Tracks

        part_column = 'Composer'
        ord_column = 'Milliseconds'
        column = 'Bytes'

        num_of_tests = 10;
        max_frame_boundery_rows = 8;
        max_frame_boundery_range = 5_000_000;

        gen_nulls = true;
        num_nulls_ord_columns = 300;
        num_nulls_columns= 250;
        break;

    case Tables.Invoices2:
        table = JSON.parse(fs.readFileSync('./spec/chinook-db/Invoice2.json'));
        _table = Tables.Invoices2

        part_column = 'BillingCity'
        ord_column = 'Total'
        column = 'Random'

        num_of_tests = 10;
        max_frame_boundery_rows = 6;
        max_frame_boundery_range = 6;

        gen_nulls = false;
        break;

    case Tables.TracksRand:
        table = JSON.parse(fs.readFileSync('./spec/chinook-db/Track.json'));
        _table = Tables.TracksRand

        part_column = 'Composer'
        ord_column = 'Milliseconds'
        column = 'Bytes'

        num_of_tests = 10;
        max_frame_boundery_rows = 8;
        max_frame_boundery_range = 1_000_000;

        gen_rands = true;
        num_rands = 50;

        gen_nulls = true;
        num_nulls_ord_columns = 300;
        num_nulls_columns= 250;

        logTable = true
        break;

    default:
        console.log('no test_table chosen');
        break;
}


if (gen_rands) {
    const rands = randoms(len(table), num_rands)
    each ((rand, ind) => table[ind][part_column] = rand) (rands)
}

if (gen_nulls) {
    const null_indexes_ord_columns = randoms(num_nulls_ord_columns, len(table) - 1)
    const null_indexes_columns = randoms(num_nulls_columns, len(table) - 1)

    each(ind => {
        table[ind][ord_column] = null;
    })(null_indexes_ord_columns)

    each(ind => {
        table[ind][column] = null;
    })(null_indexes_columns)
}



createTable(db, table, _table)




describe('SELECT -> aggregate window functions', () => {
     // ROWS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
    const win1 = OVER()
    const win2 = OVER(ORDER_BY(ord_column))

    // RANGE_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
    const win3 = OVER(PARTITION_BY(part_column))
    const win4 = OVER(PARTITION_BY(part_column), ORDER_BY(ord_column))

    const wins = [win1, win2, win3, win4]

    const _win1 = `OVER()`
    const _win2 = `OVER(ORDER BY ${ ord_column })`
    const _win3 = `OVER(PARTITION BY ${ part_column })`
    const _win4 = `OVER(PARTITION BY ${ part_column } ORDER BY ${ ord_column })`
    const _wins = [_win1, _win2, _win3, _win4]

    let win, _win, res, expected;


    it(`default frames`, () => {
        for (let ind = 0; ind < wins.length; ind++) {
            win = wins[ind];

            res =
            SELECT(
                part_column, ord_column, column,
                COUNT(column, win, AS('count')),
                SUM(column, win, AS('sum')),
                AVG(column, win, AS('avg')),
                MAX(column, win, AS('max')),
                MIN(column, win, AS('min')),
            FROM(table));

            _win = _wins[ind]

            expected = db.prepare(`
            SELECT
                ${ part_column }, ${ ord_column }, ${ column },
                COUNT(${ column }) ${ _win }  AS count,
                SUM(${ column }) ${ _win }  AS sum,
                AVG(${ column }) ${ _win }  AS avg,
                MAX(${ column }) ${ _win }  AS max,
                MIN(${ column }) ${ _win }  AS min
            FROM ${ _table }`).all();

            expect(res).toEqual(expected);
        }
    });


            
    it(`frames`, () => {
        let frame1, frame2, frame3, frame4, frame5, frame6, frame7;
        let frame8, frame9, frame10, frame11, frame12, frame13;
    
        let _frame1, _frame2, _frame3, _frame4, _frame5, _frame6, _frame7;
        let _frame8, _frame9, _frame10, _frame11, _frame12, _frame13;
    
        let res;
        let expected;
        let win;
        let _win;

        let num_ASC = 0
        let num_DESC = 0
        let num_rows = 0
        let num_ranges = 0
        let num_groups = 0

    
        for(let index = 0; index < num_of_tests; index++) {
            const frameFns  = [ROWS_BETWEEN, RANGE_BETWEEN, GROUPS_BETWEEN]
            const frameStrs = ['ROWS BETWEEN', 'RANGE BETWEEN', 'GROUPS BETWEEN']
            const rand_frames = random(0, 2)
            const frameFn = frameFns[rand_frames];
            const frameStr = frameStrs[rand_frames];

            const randFn = rand_frames === 1 ? randomsFloat : randoms;
            const max_frame_boundery = rand_frames === 1 ? max_frame_boundery_range : max_frame_boundery_rows;
            const frame_bounderies = sort(randFn(3, 0, max_frame_boundery));
            const numPrec = frame_bounderies[0]
            const num = frame_bounderies[1]
            const numFoll = frame_bounderies[2]
        
        
            const orderFns = [ASC, DESC]
            const orderStrs = ['ASC', 'DESC']
            const rand_orders = random(0, 1)
            const orderFn = orderFns[rand_orders]
            const orderStr = orderStrs[rand_orders]
        
            frame1 = frameFn(UNBOUNDED_PRECEDING, num, PRECEDING)
            frame2 = frameFn(UNBOUNDED_PRECEDING, CURRENT_ROW)
            frame3 = frameFn(UNBOUNDED_PRECEDING, num, FOLLOWING)
            frame4 = frameFn(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
    
            frame5 = frameFn(num, PRECEDING, numPrec, PRECEDING)
            frame6 = frameFn(num, PRECEDING, CURRENT_ROW)
            frame7 = frameFn(num, PRECEDING, num, FOLLOWING)
            frame8 = frameFn(num, PRECEDING, UNBOUNDED_FOLLOWING)
    
            frame9 = frameFn(CURRENT_ROW, CURRENT_ROW)
            frame10 = frameFn(CURRENT_ROW, num, FOLLOWING)
            frame11 = frameFn(CURRENT_ROW, UNBOUNDED_FOLLOWING)
    
            frame12 = frameFn(num, FOLLOWING, numFoll, FOLLOWING)
            frame13 = frameFn(num, FOLLOWING, UNBOUNDED_FOLLOWING)
    
            const frames = [
                frame1, frame2, frame3, frame4, frame5, frame6, frame7,
                frame8, frame9, frame10, frame11, frame12, frame13
            ];
    
    
            _frame1 = `${ frameStr } UNBOUNDED PRECEDING AND ${num} PRECEDING`
            _frame2 = `${ frameStr } UNBOUNDED PRECEDING AND CURRENT ROW`
            _frame3 = `${ frameStr } UNBOUNDED PRECEDING AND ${num} FOLLOWING`
            _frame4 = `${ frameStr } UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    
            _frame5 = `${ frameStr } ${num} PRECEDING AND ${numPrec} PRECEDING`
            _frame6 = `${ frameStr } ${num} PRECEDING AND CURRENT ROW`
            _frame7 = `${ frameStr } ${num} PRECEDING AND ${num} FOLLOWING`
            _frame8 = `${ frameStr } ${num} PRECEDING AND UNBOUNDED FOLLOWING`
    
            _frame9 = `${ frameStr } CURRENT ROW AND CURRENT ROW`
            _frame10 = `${ frameStr } CURRENT ROW AND ${num} FOLLOWING`
            _frame11 = `${ frameStr } CURRENT ROW AND UNBOUNDED FOLLOWING`
    
            _frame12 = `${ frameStr } ${num} FOLLOWING AND ${numFoll} FOLLOWING`
            _frame13 = `${ frameStr } ${num} FOLLOWING AND UNBOUNDED FOLLOWING`
    
            const _frames = [
                _frame1, _frame2, _frame3, _frame4, _frame5, _frame6, _frame7,
                _frame8, _frame9, _frame10, _frame11, _frame12, _frame13
            ];
    
    
            for (let ind = 0; ind < frames.length; ind++) {
                // console.log(ind, num, numPrec, frames[ind])
                win = OVER(PARTITION_BY(part_column), ORDER_BY(ord_column, orderFn), frames[ind])
                _win = `OVER (PARTITION BY ${ part_column } ORDER BY ${ ord_column } ${ orderStr } ${ _frames[ind] })`

                res =
                SELECT(
                    part_column, ord_column, column,
                    COUNT(column, win, AS('count')),
                    SUM(column, win, AS('sum')),
                    AVG(column, win, AS('avg')),
                    MAX(column, win, AS('max')),
                    MIN(column, win, AS('min')),
                FROM(table));
    
                expected = db.prepare(`
                SELECT
                    ${ part_column }, ${ ord_column }, ${ column },
                    COUNT(${ column }) ${ _win }  AS count,
                    SUM(${ column }) ${ _win }  AS sum,
                    AVG(${ column }) ${ _win }  AS avg,
                    MAX(${ column }) ${ _win }  AS max,
                    MIN(${ column }) ${ _win }  AS min
                FROM ${ _table }`).all();

                expect(res).toEqual(expected);
            }

            if (log) {
                if (orderStr === 'ASC') num_ASC++;
                else num_DESC++;
                if (frameStr === 'ROWS BETWEEN') num_rows++;
                else if (frameStr === 'RANGE BETWEEN') num_ranges++;
                else num_groups++;    
            }
        }

        if (log) {
            console.log('\nASC:', num_ASC)
            console.log('DESC:', num_DESC)
            console.log('ROWS BETWEEN:', num_rows)
            console.log('RANGE BETWEEN:', num_ranges)
            console.log('GROUPS BETWEEN:', num_groups)
        }
        if (logTable) {
            console.log(drawTable(res, { 1: 100 }))
        }
    });
});


describe('SELECT -> non-aggregate window functions', () => {

    const win = OVER(PARTITION_BY(part_column), ORDER_BY(ord_column))
    const _win = `OVER(PARTITION BY ${ part_column } ORDER BY ${ ord_column })`

    it(`ranking functions`, () => {
        const res =
        SELECT(
            part_column, ord_column, column,
            ROW_NUMBER(win, AS('row_num')),
            RANK(win, AS('rank')),
            DENSE_RANK(win, AS('dense_rank')),
        FROM(table));




        const expected = db.prepare(`
        SELECT
            ${ part_column }, ${ ord_column }, ${ column },
            ROW_NUMBER() ${ _win } AS row_num,
            RANK() ${ _win } AS rank,
            DENSE_RANK() ${ _win } AS dense_rank
        FROM ${ _table }`).all();

        expect(res).toEqual(expected);
    });

    it(`distribution functions`, () => {
        const res =
        SELECT(
            part_column, ord_column, column,
            PERCENT_RANK(win, AS('perc_rank')),
            CUME_DIST(win, AS('cume_dist')),
        FROM(table));




        const expected = db.prepare(`
        SELECT
            ${ part_column }, ${ ord_column }, ${ column },
            PERCENT_RANK() ${ _win } AS perc_rank,
            CUME_DIST() ${ _win } AS cume_dist
        FROM ${ _table }`).all();

        expect(res).toEqual(expected);
    });

    it(`analytic functions -> without frames`, () => {
        const res =
        SELECT(
            part_column, ord_column, column,
            LEAD(column, win, AS('lead')),
            LEAD(column, 2, 0, win, AS('lead_2_0')),
            LAG(column, win, AS('lag')),
            LAG(column, 2, 0, win, AS('lag_2_0')),
            NTILE(3, win, AS('ntile')),
        FROM(table));




        const expected = db.prepare(`
        SELECT
            ${ part_column }, ${ ord_column }, ${ column },
            LEAD(${ column }) ${ _win } AS lead,
            LEAD(${ column }, 2, 0) ${ _win } AS lead_2_0,
            LAG(${ column }) ${ _win } AS lag,
            LAG(${ column }, 2, 0) ${ _win } AS lag_2_0,
            NTILE(3) ${ _win } AS ntile
        FROM ${ _table }`).all();

        expect(res).toEqual(expected);
    });

    it(`analytic functions -> with frames`, () => {
        const res =
        SELECT(
            part_column, ord_column, column,
            FIRST_VALUE(column, win, AS('first_value')),
            LAST_VALUE(column, win, AS('last_value')),
            NTH_VALUE(column, 3, win, AS('nth_value')),
        FROM(table));



        const _win2 = `OVER(PARTITION BY ${ part_column } ORDER BY ${ ord_column }
                            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`

        const expected = db.prepare(`
        SELECT
            ${ part_column }, ${ ord_column }, ${ column },
            FIRST_VALUE(${ column }) ${ _win } AS first_value,
            LAST_VALUE(${ column }) ${ _win2 } AS last_value,
            NTH_VALUE(${ column }, 3) ${ _win2 } AS nth_value
        FROM ${ _table }`).all();



        expect(res).toEqual(expected);
    });
});

