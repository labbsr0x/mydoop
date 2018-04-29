const log = require('debug')('query-executor-log')
const debug = require('debug')('query-executor-debug')
const parseUtils = require('./parse-utils')
const querySplitter = require('./query-splitter')

var mysql = require('mysql');
var moment = require('moment');
var async = require('async')

module.exports = {

    /**
     * Simple executes the query to the database
     * 
     * @returns Array [] with the resultset from database
     */
    executeInDatabase: function (sql) {
        debug('query::', sql)

        const mysqlHost = process.env.MYSQL_HOST || '172.20.54.11'
        const mysqlPort = process.env.MYSQL_PORT || '3306'
        const mysqlUser = process.env.MYSQL_USER || 'tiago'
        const mysqlPassword = process.env.MYSQL_PASSWORD || 'qwe123'
        const mysqlDatabase = process.env.MYSQL_DATABASE || 'PIWIK'

        var connection = mysql.createConnection({
            host: mysqlHost,
            port: mysqlPort,
            user: mysqlUser,
            password: mysqlPassword,
            database: mysqlDatabase
        });

        return new Promise((resolve, reject) => {

            connection.connect((err, args) => {
                if (err) {
                    console.error(err)
                    reject(error)
                    throw err
                }
                connection.query(sql, function (error, results, fields) {
                    if (error) {
                        console.error(error);
                        reject(error)
                        throw error;
                    }
                    log('results: ', results);
                    resolve(results)
                });
            })

        })

        // return new Promise((resolve, reject) => {
        //     // if the query has DISTINCT constraint, than it should be resolved on another pipeline
        //     // after all the result sets are merged, because the distinct aggregation cannot be made in isolated batches
        //     if (query.aggregationType == 'NONE' || query.distinct || query.aggregationType == 'MAX' || query.aggregationType == 'MIN') {
        //         connectAndExecute(query, r => resolve(r))
        //     }else{                
        //         // if the query is an aggregation of COUNT or SUM and the is no "distinct" constraint
        //         // then the aggregation can b
        //         connectAndExecute(query, r => {
        //             if (query.aggregationType.toUpperCase() == 'COUNT') {                                                
        //                 const cnt = [...new Set(r)].length
        //                 debug('AGGREGATION TYPE :COUNT:', cnt)    
        //                 resolve(cnt)
        //             }else if (query.aggregationType.toUpperCase() == 'SUM') {
        //                 const sum = r.reduce((a, b) => a + Object.values(b)[0], 0)
        //                 debug('AGGREGATION TYPE :SUM:', sum)    
        //                 resolve(sum)
        //             }else{
        //                 resolve(r)
        //             }
        //         })
        //     }
        // })

    },

    /**
     * Execute the queries and "post-process" depending on the `type` attribute of the query
     * 
     * @returns Array [] with the result of the queries 
     */
    executeParallelQueries: function(queries) {
        debug('queries::', queries)

        async.map(queries, async q => {
            log('Executing QUERY::', q)
            return await this.execute(q)
        }, (err, data) => {
            // console.log('data', data);

            // console.log(new Date().getTime()-time);
            // const res = [...new Set(data.reduce((a, b) => a.concat(b)))] ==>>> Para poder fazer o DISTINCT, tem que colocar num SET
            const res = data.reduce((a, b) => a.concat(b))
            // console.log('res', res.length);

            callback(res)
        })
    }
}