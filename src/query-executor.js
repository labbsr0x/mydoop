const log = require('debug')('query-executor-log')
const debug = require('debug')('query-executor-debug')
const parseUtils = require('./parse-utils')
const querySplitter = require('./query-splitter')

var mysql = require('mysql');
var moment = require('moment');
var async = require('async')

module.exports = {

    /**
     * Executes a query and make any needed post-resultset aggregation
     * 
     * @returns Array [] with the result set post-processed
     */
    execute: function (query) {
        debug('query::', query)

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

        const connectAndExecute = (cb) => {
            connection.connect((err, args) => {
                if (err) {
                    console.error(err)
                    reject(error)
                    throw err
                }
                connection.query(query.sql, function (error, results, fields) {
                    if (error) {
                        console.error(error);
                        reject(error)
                        throw error;
                    }
                    log('results: ', results);
                    cb(results)
                });
            })
        }
        return new Promise((resolve, reject) => {
            if (query.aggregationType == 'DIRECT') {
                connectAndExecute(r => resolve(r))
            }else{
                connectAndExecute(r => {
                    resolve(r)
                })
            }
        })

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