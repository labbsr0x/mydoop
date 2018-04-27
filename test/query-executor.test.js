const assert = require('assert');
const queryExecutor = require('../src/query-executor')

const queries = [
    {
        sql: `select table_mock_alias.column_1,table_mock_alias.column_2,count(*),sum(table_mock_alias.column_4),max(table_mock_alias.column_4),sum(table_mock_alias.column_5),sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end),sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
        type: 'DIRECT'
    },
    {
        sql: `select distinct table_mock_alias.column_3 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
        type: 'COUNT'
    },
    {
        sql: `select distinct table_mock_alias.column_7 from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:10:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
        type: 'COUNT'
    }
]

describe('queryExecutor', () => {
    describe('#execute()', () => {
        it('should execute a direct query and return a resultset as array', () => {
            // queryExecutor.execute(queries[0])
        })
    });
});
