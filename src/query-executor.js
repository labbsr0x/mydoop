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

    },

    mergeSQLWithParams: function (sql, rowResultRefParam) {
        //get the parameters
        const params = sql.match(/\{(.*?)\}/g)
        if (!params) {
            return sql
        }
        const columnNamesParam = params.map(p => p.replace(/[\{|\}]/g, '')) // get the param without the `{}` to be able to get the attribute from the resultset
        columnNamesParam.forEach(c => {
            const refValue = typeof (rowResultRefParam[c]) == "string" ? `'${rowResultRefParam[c]}'` : rowResultRefParam[c]
            sql = sql.replace(new RegExp(`\{(${c})\}`), refValue)
        })
        return sql  
    },

    executeDistributed: function(sql, rowResultRefParam) {
        return new Promise((resolve, reject) => {
            
            const mergedSQL = this.mergeSQLWithParams(sql, rowResultRefParam)
            // find the columns that has range comparasion to distribute the load

        })
    },

    /**
     * Execute the composition of queries and "post-process" depending on the `type` attribute of the query.
     * The strategy is to first run the master query, then, with its results, find the GROUP BY column values on the resultset
     * and then run the derived aggregation queries for each of those GROUP BY values. That's where the parallelism goes.!
     * 
     * @returns Array [] with the result of the queries 
     */
    executeComposedQueries: async function(queries) {
        debug('queries::', queries)

        return new Promise(async (resolve, reject) => {

            const masterQuery = queries.filter(q => q.role == "MASTER")[0]
            const aggregationQueries = queries.filter(q => q.role == "AGGREGATION")
            if (!masterQuery) {
                reject('At least one MASTER query must be provided to run parallel queries')
                throw new Error('At least one MASTER query must be provided to run parallel queries')
            }

            //
            //-- first, we run the MASTER query to get the GROUP BY column values to use in the AGGREGATION queries, derived from the original query
            const result = await this.executeDistributed(masterQuery)
            if (aggregationQueries.length==0) {
                return resolve(result)
            }

            //-- with the result of the MASTER query, we will execute the AGGREGATION queries to compose the final resultset
            result.forEach(row => {
                // -- THIS IS WHERE IN-CODE AGGREGATION HAPPENS

                aggregationQueries.forEach(async aq => {
                    //will replace the `-1` placed in the original query with the actual value from the in-code aggregation
                    let aggResult = await this.executeDistributed(aq.sql, row) //pass the row parameter to get the value to the columns that are in the `where` clause                
                    //in-code aggregation
                    if (aq.aggregationType.toUpperCase() == 'COUNT') {                                                
                        const cnt = aq.distinct ? [...new Set(aggResult)].length : aggResult.length
                        debug('AGGREGATION TYPE :COUNT:', cnt)    
                        row[aq.targetColumn] = cnt

                    }else if (query.aggregationType.toUpperCase() == 'SUM') {
                        const sum = aq.distinct ? [...new Set(aggResult)].reduce((a, b) => a + Object.values(b)[0], 0) : aggResult.reduce((a, b) => a + Object.values(b)[0], 0)
                        debug('AGGREGATION TYPE :SUM:', sum)    
                        row[aq.targetColumn] = sum

                    } else if (query.aggregationType.toUpperCase() == 'MAX') {
                        reject('in-code MAX Aggregation not yet supported')
                        throw new Error('in-code MAX Aggregation not yet supported')                        
                    } else if (query.aggregationType.toUpperCase() == 'MIN') {
                        reject('in-code MIN Aggregation not yet supported')
                        throw new Error('in-code MIN Aggregation not yet supported')                        
                    } else if (query.aggregationType.toUpperCase() == 'AVG') {
                        reject('in-code AVG Aggregation not yet supported')
                        throw new Error('in-code AVG Aggregation not yet supported')                                                                        
                    }else{
                        row[aq.targetColumn] = -1 //the aggregation was made in the DB
                    }
                })
            })

        })

        // async.map(queries, async q => {
        //     log('Executing QUERY::', q)
        //     return await this.execute(q)
        // }, (err, data) => {
        //     // console.log('data', data);

        //     // console.log(new Date().getTime()-time);
        //     // const res = [...new Set(data.reduce((a, b) => a.concat(b)))] ==>>> Para poder fazer o DISTINCT, tem que colocar num SET
        //     const res = data.reduce((a, b) => a.concat(b))
        //     // console.log('res', res.length);

        //     callback(res)
        // })
    }
}