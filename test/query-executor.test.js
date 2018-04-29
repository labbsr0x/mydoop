const assert = require('assert');
const queryExecutor = require('../src/query-executor')

describe('queryExecutor', () => {

    describe('#mergeSQLWithParams()', () => {
        it('should keep the SQL unmodified as it doesnt has special params', () => {
            const targetQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44')`
            const resultSetMock = {
                '1': 'COL--1',
                '2': 'COL--2',
                '3': -1,
                '4': 4,
                '5': 268,
                '6': 222,
                '7': 401,
                '8': 0,
                '9': 0,
                '10': -1
            }
            const finalQuery = queryExecutor.mergeSQLWithParams(targetQuery, resultSetMock)            
            assert.equal(finalQuery, targetQuery)
        })
        it('should replace the SQL column placeholders with the values from the other resultset', () => {
            const targetQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}`
            const resultSetMock = {
                '1': 'COL--1',
                '2': 'COL--2',
                '3': -1,
                '4': 4,
                '5': 268,
                '6': 222,
                '7': 401,
                '8': 0,
                '9': 0,
                '10': -1
            }
            const finalQuery = queryExecutor.mergeSQLWithParams(targetQuery, resultSetMock)
            assert.equal(finalQuery, `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`)            
        })
    });
    describe('#execute()', () => {
        it('should execute a NONE query and return a resultset as array', async () => {
            const query = {
                sql: `select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
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
                sql: `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
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
                sql: `select distinct table_mock_alias.column_8 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
                aggregationType: 'COUNT',
                distinct: true,
                role: 'AGGREGATION',
                term: 'count(distinct table_mock_alias.column_8)',
                targetColumn: 'table_mock_alias.column_8'
            }
            const res = await queryExecutor.executeInDatabase(query)
            assert.equal(res.length, 2)
        })
    })
});
