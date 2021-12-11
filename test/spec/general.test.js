import { SELECT, FROM, JOIN, ORDER_BY, ADD, AS, WHERE, OVER, SELECT_TOP, ID, USING, DESC, COUNT,
    GROUP_BY, WITHIN_GROUP, PARTITION_BY, PERCENTILE_CONT, PERCENTILE_DISC, MEDIAN } from '../../dist/select.js'
import { createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');

import fs from 'fs'
let invoices2 = JSON.parse(fs.readFileSync('./spec/chinook-db/Invoice2.json'));

createTable(db, invoices2, 'invoices2')


let tracks = JSON.parse(fs.readFileSync('./spec/chinook-db/Track.json'));
let albums = JSON.parse(fs.readFileSync('./spec/chinook-db/Album.json'));
let artists = JSON.parse(fs.readFileSync('./spec/chinook-db/Artist.json'));
let genres = JSON.parse(fs.readFileSync('./spec/chinook-db/Genre.json'));

createTable(db, tracks, 'tracks')
createTable(db, albums, 'albums')
createTable(db, artists, 'artists')
createTable(db, genres, 'genres')


describe('generals', () => {
    it(`1`, () => {
        const table = `
+---------------+-------+--------+-----------+-----------+--------+-------+--------+
| BillingCity   | Total | Random | perc_cont | perc_disc | median | added | added2 |
+---------------+-------+--------+-----------+-----------+--------+-------+--------+
| Cupertino     | 13.86 |   null |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  1.98 |   null |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  1.99 |   null |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  null |   null |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  null |     50 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  null |     50 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  1.98 |     60 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  8.91 |     60 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  null |     60 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  5.94 |     70 |        60 |        60 |     60 |   120 |    540 |
| Cupertino     |  3.96 |     80 |        60 |        60 |     60 |   120 |    540 |
| Mountain View |  1.98 |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View | 13.86 |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  8.91 |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  3.96 |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  null |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  null |   null |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  0.99 |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View | 13.86 |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  3.98 |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  3.96 |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  5.94 |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  null |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  null |     50 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  1.98 |     60 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  1.98 |     60 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  5.94 |     60 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  null |     60 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  0.99 |     70 |        50 |        50 |     50 |   100 |    450 |
| Mountain View |  8.91 |     80 |        50 |        50 |     50 |   100 |    450 |
+---------------+-------+--------+-----------+-----------+--------+-------+--------+
30 rows selected`;
        
        const wg = WITHIN_GROUP(ORDER_BY('Random'))
        const win = OVER(PARTITION_BY('BillingCity'))

        const res =
        SELECT(
            'BillingCity', 'Total', 'Random',
            PERCENTILE_CONT(0.5, wg, win, AS('perc_cont')),
            PERCENTILE_DISC(0.5, wg, win, AS('perc_disc')),
            MEDIAN('Random', win, AS('median')),
            ADD('perc_cont', 'perc_disc', AS('added')),
            ADD(
                PERCENTILE_CONT(0.5, wg, win),
                PERCENTILE_DISC(0.5, wg, win),
                MEDIAN('Random', win),
                ADD(
                    PERCENTILE_CONT(0.5, wg, win),
                    PERCENTILE_DISC(0.5, wg, win),
                    MEDIAN('Random', win),
                    ADD(
                        PERCENTILE_CONT(0.5, wg, win),
                        PERCENTILE_DISC(0.5, wg, win),
                        MEDIAN('Random', win),    
                    )              
                ),
                AS('added2')
            ),
        FROM(invoices2));
        
        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`2`, () => {
        const table = `
+---------------+-----------+-----------+--------+-------+--------+
| BillingCity   | perc_cont | perc_disc | median | added | added2 |
+---------------+-----------+-----------+--------+-------+--------+
| Cupertino     |        60 |        60 |     60 |   120 |    540 |
| Mountain View |        50 |        50 |     50 |   100 |    450 |
+---------------+-----------+-----------+--------+-------+--------+
2 rows selected`;
        
        const wg = WITHIN_GROUP(ORDER_BY('Random'))

        const res =
        SELECT(
            'BillingCity',
            PERCENTILE_CONT(0.5, wg, AS('perc_cont')),
            PERCENTILE_DISC(0.5, wg, AS('perc_disc')),
            MEDIAN('Random', AS('median')),
            ADD('perc_cont', 'perc_disc', AS('added')),
            ADD(
                PERCENTILE_CONT(0.5, wg),
                PERCENTILE_DISC(0.5, wg),
                MEDIAN('Random'),
                ADD(
                    PERCENTILE_CONT(0.5, wg),
                    PERCENTILE_DISC(0.5, wg),
                    MEDIAN('Random'),
                    ADD(
                        PERCENTILE_CONT(0.5, wg),
                        PERCENTILE_DISC(0.5, wg),
                        MEDIAN('Random'),    
                    )              
                ),
                AS('added2')
            ),
        FROM(invoices2),
        GROUP_BY('BillingCity'));
                
        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`3`, () => {
        const table = `
+-----------+-----------+--------+-------+--------+
| perc_cont | perc_disc | median | added | added2 |
+-----------+-----------+--------+-------+--------+
|        60 |        60 |     60 |   120 |    540 |
+-----------+-----------+--------+-------+--------+
1 row selected`;
        
        const wg = WITHIN_GROUP(ORDER_BY('Random'))

        const res =
        SELECT(
            PERCENTILE_CONT(0.5, wg, AS('perc_cont')),
            PERCENTILE_DISC(0.5, wg, AS('perc_disc')),
            MEDIAN('Random', AS('median')),
            ADD('perc_cont', 'perc_disc', AS('added')),
            ADD(
                PERCENTILE_CONT(0.5, wg),
                PERCENTILE_DISC(0.5, wg),
                MEDIAN('Random'),
                ADD(
                    PERCENTILE_CONT(0.5, wg),
                    PERCENTILE_DISC(0.5, wg),
                    MEDIAN('Random'),
                    ADD(
                        PERCENTILE_CONT(0.5, wg),
                        PERCENTILE_DISC(0.5, wg),
                        MEDIAN('Random'),    
                    )              
                ),
                AS('added2')
            ),
        FROM(invoices2));
                
        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`4`, () => {
        const table = `
+------------------------------+------------+
| ArtistName                   | TrackCount |
+------------------------------+------------+
| Led Zeppelin                 |        114 |
| U2                           |        112 |
| Deep Purple                  |         92 |
| Iron Maiden                  |         81 |
| Pearl Jam                    |         54 |
| Van Halen                    |         52 |
| Queen                        |         45 |
| The Rolling Stones           |         41 |
| Creedence Clearwater Revival |         40 |
| Kiss                         |         35 |
+------------------------------+------------+
10 rows selected`;
        
        const res =
        SELECT_TOP(10)(
            ID('t3.Name', AS('ArtistName')),
            COUNT('TrackId', AS('TrackCount')),
        FROM(tracks, JOIN, albums, USING('AlbumId'),
                     JOIN, artists, USING('ArtistId'),
                     JOIN, genres, USING('GenreId')),
        WHERE(r => r['t4.GenreId'] === 1),
        GROUP_BY('ArtistId', 'ArtistName'),
        ORDER_BY('TrackCount', DESC))
                        
        const expected =
        db.prepare(`
        SELECT ar.Name AS ArtistName,
            COUNT(TrackId) AS TrackCount
        FROM tracks AS t JOIN albums AS al ON(t.AlbumId = al.AlbumId)
                         JOIN artists AS ar ON(al.ArtistId = ar.ArtistId)
                         JOIN genres AS g ON(t.GenreId = g.GenreId)
        WHERE g.GenreId = 1
        GROUP BY al.ArtistId
        ORDER BY TrackCount DESC
        LIMIT 10`).all();
        
        expect(trim(drawTable(res))).toEqual(trim(table));
        expect(res).toEqual(expected);
    });

it(`5`, () => {
        const table = `
+---------------+-------+--------+
| BillingCity   | Total | Random |
+---------------+-------+--------+
| Mountain View |  0.99 |     50 |
| Cupertino     |  1.98 |     60 |
| Cupertino     | 13.86 |   null |
| Cupertino     |  8.91 |     60 |
| Mountain View |  1.98 |   null |
| Mountain View | 13.86 |   null |
| Mountain View |  1.98 |     60 |
| Mountain View | 13.86 |     50 |
| Mountain View |  8.91 |     80 |
| Mountain View |  8.91 |   null |
| Cupertino     |  1.98 |   null |
| Cupertino     |  3.96 |     80 |
| Cupertino     |  5.94 |     70 |
| Cupertino     |  1.99 |   null |
| Mountain View |  3.98 |     50 |
| Mountain View |  1.98 |     60 |
| Mountain View |  3.96 |   null |
| Mountain View |  3.96 |     50 |
| Mountain View |  5.94 |     50 |
| Mountain View |  5.94 |     60 |
| Mountain View |  0.99 |     70 |
| Cupertino     |  null |     50 |
| Cupertino     |  null |   null |
| Cupertino     |  null |     50 |
| Cupertino     |  null |     60 |
| Mountain View |  null |   null |
| Mountain View |  null |     50 |
| Mountain View |  null |   null |
| Mountain View |  null |     50 |
| Mountain View |  null |     60 |
+---------------+-------+--------+
30 rows selected
`

        const res =
        SELECT(
            'BillingCity', 'Total', 'Random',
        FROM(invoices2));

        const expected = db.prepare(`
        SELECT
            BillingCity, Total, Random
        FROM invoices2`).all();

        expect(res).toEqual(expected);
        expect(trim(drawTable(res))).toEqual(trim(table));
    });

    it(`percentile window functions`, () => {
        const expected = `
+---------------+-------+--------+-----------+-----------+--------+
| BillingCity   | Total | Random | perc_cont | perc_disc | median |
+---------------+-------+--------+-----------+-----------+--------+
| Cupertino     | 13.86 |   null |        60 |        60 |     60 |
| Cupertino     |  1.98 |   null |        60 |        60 |     60 |
| Cupertino     |  1.99 |   null |        60 |        60 |     60 |
| Cupertino     |  null |   null |        60 |        60 |     60 |
| Cupertino     |  null |     50 |        60 |        60 |     60 |
| Cupertino     |  null |     50 |        60 |        60 |     60 |
| Cupertino     |  1.98 |     60 |        60 |        60 |     60 |
| Cupertino     |  8.91 |     60 |        60 |        60 |     60 |
| Cupertino     |  null |     60 |        60 |        60 |     60 |
| Cupertino     |  5.94 |     70 |        60 |        60 |     60 |
| Cupertino     |  3.96 |     80 |        60 |        60 |     60 |
| Mountain View |  1.98 |   null |        50 |        50 |     50 |
| Mountain View | 13.86 |   null |        50 |        50 |     50 |
| Mountain View |  8.91 |   null |        50 |        50 |     50 |
| Mountain View |  3.96 |   null |        50 |        50 |     50 |
| Mountain View |  null |   null |        50 |        50 |     50 |
| Mountain View |  null |   null |        50 |        50 |     50 |
| Mountain View |  0.99 |     50 |        50 |        50 |     50 |
| Mountain View | 13.86 |     50 |        50 |        50 |     50 |
| Mountain View |  3.98 |     50 |        50 |        50 |     50 |
| Mountain View |  3.96 |     50 |        50 |        50 |     50 |
| Mountain View |  5.94 |     50 |        50 |        50 |     50 |
| Mountain View |  null |     50 |        50 |        50 |     50 |
| Mountain View |  null |     50 |        50 |        50 |     50 |
| Mountain View |  1.98 |     60 |        50 |        50 |     50 |
| Mountain View |  1.98 |     60 |        50 |        50 |     50 |
| Mountain View |  5.94 |     60 |        50 |        50 |     50 |
| Mountain View |  null |     60 |        50 |        50 |     50 |
| Mountain View |  0.99 |     70 |        50 |        50 |     50 |
| Mountain View |  8.91 |     80 |        50 |        50 |     50 |
+---------------+-------+--------+-----------+-----------+--------+
30 rows selected
`

        const res =
        SELECT(
            'BillingCity', 'Total', 'Random',
            PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('Random')), OVER(PARTITION_BY('BillingCity')), AS('perc_cont')),
            PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('Random')), OVER(PARTITION_BY('BillingCity')), AS('perc_disc')),
            MEDIAN('Random', OVER(PARTITION_BY('BillingCity')), AS('median')),
        FROM(invoices2));



        expect(trim(drawTable(res))).toEqual(trim(expected));

    });





});
