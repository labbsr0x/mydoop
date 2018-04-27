const assert = require('assert');
const querySplitter = require('../src/query-splitter')

describe('querySplitter', () => {
    describe('#separateQueriesByDistinct()', () => {
        let query = `SELECT
                        table_mock_alias.column_1 , 
                        table_mock_alias.column_2 , 
                        count(distinct table_mock_alias.column_3) , 
                        count(*) , 
                        sum(table_mock_alias.column_4) , 
                        max(table_mock_alias.column_4) , 
                        sum(table_mock_alias.column_5) , 
                        sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) , 
                        sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) , 
                        count(distinct table_mock_alias.column_7) 
                        FROM
                        table_mock AS table_mock_alias
                        WHERE
                        table_mock_alias.time_column_1 >= '2018-04-26 11:00:00'
                        AND table_mock_alias.time_column_1 <= '2018-04-26 11:10:00'
                        AND table_mock_alias.column_7 IN ('44') 
                        GROUP BY table_mock_alias.column_1, table_mock_alias.column_2
                    `

        let result = querySplitter.separateQueriesByDistinct(query)

        it('should return 3 queries', () => {
            assert.equal(result.length, 3)
        })
        it('should return 1 MAIN query extracted', () => {
            assert.equal(result[0].sql, `select table_mock_alias.column_1,table_mock_alias.column_2,count(*),sum(table_mock_alias.column_4),max(table_mock_alias.column_4),sum(table_mock_alias.column_5),sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end),sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`)
        })
        it('should return 2 AGGREGATION queries extracted', () => {
            assert.equal(result[1].sql, `select distinct table_mock_alias.column_3 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44')`)
            assert.equal(result[2].sql, `select distinct table_mock_alias.column_7 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44')`)
        })
        it('should return 1 DIRECT query extracted', () => {
            assert.equal(result[0].type, 'DIRECT')
        })
        it('should return 2 COUNT queries extracted', () => {
            assert.equal(result[1].type, 'COUNT')
            assert.equal(result[2].type, 'COUNT')
        })
        
    });
});
