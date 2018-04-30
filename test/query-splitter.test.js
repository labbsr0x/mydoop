const assert = require('assert');
const querySplitter = require('../src/query-splitter')

describe('querySplitter', () => {
    describe('#separateQueriesByDistinct()', () => {
        let query = `SELECT
                        table_mock_alias.column_1 as "1", 
                        table_mock_alias.column_2 as "2", 
                        count(distinct table_mock_alias.column_3) as "3", 
                        count(*) as "4", 
                        sum(table_mock_alias.column_4) as "5", 
                        max(table_mock_alias.column_4) as "6", 
                        sum(table_mock_alias.column_5) as "7", 
                        sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8", 
                        sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9", 
                        count(distinct table_mock_alias.column_8) as "10"
                        FROM
                        table_mock AS table_mock_alias
                        WHERE
                        table_mock_alias.time_column_1 >= '2018-04-26 11:00:00'
                        AND table_mock_alias.time_column_1 <= '2018-04-26 23:40:00'
                        AND table_mock_alias.column_7 IN ('44') 
                        GROUP BY table_mock_alias.column_1, table_mock_alias.column_2
                    `

        let result = querySplitter.separateQueriesByDistinct(query)

        it('should return 3 queries', () => {
            assert.equal(result.length, 3)
        })
        it('should return 1 MASTER query extracted', () => {
            assert.equal(result[0].sql, `select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`)
            assert.equal(result[0].role, 'MASTER')
        })
        it('should return 2 AGGREGATION queries extracted', () => {
            assert.equal(result[1].role, 'AGGREGATION')
            assert.equal(result[2].role, 'AGGREGATION')
            assert.equal(result[1].sql, `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}`)
            assert.equal(result[2].sql, `select distinct table_mock_alias.column_8 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}`)
            assert.equal(result[1].targetColumn.expression, { "expression": 'count(distinct table_mock_alias.column_3)', "alias": "3" }.expression)
            assert.equal(result[1].targetColumn.alias, { "expression": 'count(distinct table_mock_alias.column_3)', "alias": "3" }.alias)
            assert.equal(result[2].targetColumn.expression, { "expression": 'count(distinct table_mock_alias.column_8)', "alias": "10" }.expression)
            assert.equal(result[2].targetColumn.alias, { "expression": 'count(distinct table_mock_alias.column_8)', "alias": "10" }.alias)
        })
        it('should return 1 NONE query extracted', () => {
            assert.equal(result[0].aggregationType, 'NONE')
        })
        it('should return 2 COUNT queries extracted', () => {
            assert.equal(result[1].aggregationType, 'COUNT')
            assert.equal(result[2].aggregationType, 'COUNT')
        })
        
    });
});
