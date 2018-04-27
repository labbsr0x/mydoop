const assert = require('assert');
const queryExecutor = require('../src/query-executor')

describe('queryExecutor', () => {
    describe('#execute()', () => {
        it('should execute a DIRECT query and return a resultset as array', async () => {
            const query = {
                sql: `select table_mock_alias.column_1,table_mock_alias.column_2,count(*),sum(table_mock_alias.column_4),max(table_mock_alias.column_4),sum(table_mock_alias.column_5),sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end),sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
                aggregationType: 'DIRECT'
            }
            const res = await queryExecutor.execute(query)            
            assert.equal(res.length, 3)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_3 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44')`,
                aggregationType: 'COUNT'
            }
            const res = await queryExecutor.execute(query)
            assert.equal(res.length, 2)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_7 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44')`,
                aggregationType: 'COUNT'
            }
            const res = await queryExecutor.execute(query)
            assert.equal(res.length, 1)
        })
    });
});
