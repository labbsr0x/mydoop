const assert = require('assert');
const queryExecutor = require('../src/query-executor')

describe('queryExecutor', () => {
    describe('#execute()', () => {
        it('should execute a NONE query and return a resultset as array', async () => {
            const query = {
                sql: `select table_mock_alias.column_1,table_mock_alias.column_2,-1 as "table_mock_alias.column_3",count(*),sum(table_mock_alias.column_4),max(table_mock_alias.column_4),sum(table_mock_alias.column_5),sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end),sum(case table_mock_alias.column_6 when 1 then 1 else 0 end),-1 as "table_mock_alias.column_8" from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
                aggregationType: 'NONE',
                distinct: true,
                role: 'MASTER',
                term: "FULL",
                targetColumn: "NONE"                
            }
            const res = await queryExecutor.executeInDatabase(query)            
            assert.equal(res.length, 3)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_3 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
                aggregationType: 'COUNT',
                distinct: true,
                role: 'AGGREGATION',
                term: 'count(distinct table_mock_alias.column_3)',
                targetColumn: 'table_mock_alias.column_3'
            }
            const res = await queryExecutor.executeInDatabase(query)
            assert.equal(res.length, 3)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_8 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
                aggregationType: 'COUNT',
                distinct: true,
                role: 'AGGREGATION',
                term: 'count(distinct table_mock_alias.column_8)',
                targetColumn: 'table_mock_alias.column_8'
            }
            const res = await queryExecutor.executeInDatabase(query)
            assert.equal(res.length, 2)
        })
    }),
    describe('#mergeQueryWithParams()', () => {
        it('should replace the column placeholders with the values from the other resultset', () => {
            const resultSet = {
                'column_1': 'COL--1',
                'column_2': 'COL--2',
                'table_mock_alias.column_3': -1,
                'count(*)': 4,
                'sum(table_mock_alias.column_4)': 268,
                'max(table_mock_alias.column_4)': 222,
                'sum(table_mock_alias.column_5)': 401,
                'sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end)': 0,
                'sum(case table_mock_alias.column_6 when 1 then 1 else 0 end)': 0,
                'table_mock_alias.column_8': -1
            }
        })
    })
});
