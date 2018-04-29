const info = require('debug')('query-executor-info')
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

    /**
     * Replaces parameters written as `{[row_attribute]}` in the provided SQL with the `value` of 
     * the row attribute in the correspondent attribute `row_attribute`.
     * 
     * For example:
     *  - sql=`select * from custom_table where column_1={row_attribute_2}`
     *  - row = `{ "row_attribute_1": "VAL_1", "row_attribute_2": "VAL_2" }`
     *  - mergeSQLWithParams(sql, row) => `select * from custom_table where column_1='VAL_2'`
     * 
     * @returns SQL with the placeholderParams replaced with the correspondent col values from the row
     */
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


    /**
     * @param arrTerms `Array[]` of the original WHERE clause terms to be expanded into multiple WHEREs to be distributed
     * @param secondsInterval `integer` indicating how many seconds should be used as interval between the distributed WHEREs
     * @returns `Array[]` of multiple WHERE clause string, separated by given interval
     */
    buildTimeDistributedWhereClause: function(arrTerms, secondsInterval) {
        const timedTerms = arrTerms.filter(t => t.rightTerm && !!t.rightTerm.match(/('\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')/g))
                                    .reduce((acc, term) => {
                                        if (!term.leftTerm) return acc
                                    
                                        const dateValue = new Date(term.rightTerm.replace(/\'/g, ''))     

                                        let currentTermArr = acc.filter(a => a.termName == term.leftTerm)
                                        if (!currentTermArr || currentTermArr.length==0) {
                                            let currentTerm = {}
                                            currentTerm.termName = term.leftTerm
                                            currentTerm.minValue = dateValue
                                            currentTerm.maxValue = dateValue
                                            acc.push(currentTerm)
                                        }else{
                                            if (dateValue.getTime() < currentTermArr[0].minValue.getTime()) {
                                                currentTermArr[0].minValue = dateValue
                                            }else{
                                                currentTermArr[0].maxValue = dateValue
                                            }
                                        }
                                        return acc
                                    }, [])

        debug('timedTerms::', timedTerms)                                   
        if (timedTerms.length != 1) {
            throw new Error('Currently mydoop doesnt support more than one timed column in the WHERE clause nor the BETWEEN operator')
        }
        const isTimedTerm = (term) => {
            return timedTerms.filter(t => t.termName == term).length > 0
        }
        let basicWHERE = 'where '
        basicWHERE += arrTerms.reduce((str, term) => {

            if (term.connector) { //"and" "or"
                return str += ` ${term.connector} `
            }

            let comparasionValue = term.rightTerm
            if (isTimedTerm(term.leftTerm)) {
                if (term.operator.indexOf('>') > -1) {
                   comparasionValue = `{minValue}`
                }else{
                    comparasionValue = `{maxValue}`
                }
            }
            str += `${term.leftTerm} ${term.operator} ${comparasionValue}`
            return str
        }, '')

        log('basicWHERE::', basicWHERE)

        // let start = moment()
        // let fim = inicio.clone()
        // let intervals = []
        // //montar a lista de intervalos
        // for (var i = 0; i < parallelRate; i++) {

        //     fim.add(rate, 'hour').subtract(1, 'second');
        //     intervals.push({
        //         "inicio": inicio.format('YYYY-MM-DD HH:mm:ss'),
        //         "fim": fim.format('YYYY-MM-DD HH:mm:ss')
        //     })

        //     fim.add(1, 'second');
        //     inicio.add(rate, 'hour');
        // }
    },

    /**
     * Executes a query distributedly
     * 
     * @param sql - the SQL to be executed
     * @returns a Promise for the distributed queries execution
     */
    executeDistributed: function(sql) {
        return new Promise((resolve, reject) => {
            
            let sqlWhereTokensArr = sql.split(' where ')
            if (sqlWhereTokensArr.length == 1) { //the query doesnt have `WHERE` clause.
                info('Distributing "non-where" queries is not yet supported:', sql)
                return resolve(this.executeInDatabase(sql))
            }

            // find the columns that has range comparision to distribute the load between multiple queries
            debug('sqlWhereTokensArr-where-and+::', sqlWhereTokensArr)                                       
            //tokenize the WHERE terms to find the range candidate columns to distribute
            sqlWhereTokensArr = sqlWhereTokensArr[1].split(' order by ')[0]
                                    .split(' limit ')[0]
                                    .split(' having ')[0]
                                    .split(/(\('[^'|\\']*'\)\s*|'[^'|\\']*'\s*|>=\s*|<=\s*|>\s*|<\s*|<>\s*|=\s*|and\s*|or\s*|between\s*|like\s*|in\s*)/g)
                                    
            debug('sqlWhereTokensArr-just-where::', sqlWhereTokensArr)                                    

            const whereTerms = sqlWhereTokensArr.reduce((a,b) => {
                if (b.trim() == '') return a

                if(parseUtils.isKeyWord(b)){
                    a.push({connector: b.trim()})
                    a.push({})
                }else if(parseUtils.isOperator(b)) {
                    a[a.length - 1].operator = b.trim()
                }else if (a[a.length-1].leftTerm) {
                    a[a.length - 1].rightTerm = b.trim()
                }else{
                    a[a.length - 1].leftTerm = b.trim()
                }            
                return a
            }, [{}])      
            
            debug('whereTerms::', whereTerms)

            const possibleWheres = this.buildTimeDistributedWhereClause(whereTerms, 3600)
            log('possibleWheres::', possibleWheres)

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
                    //foreach aggregation query, replace the `-1` value placed in the original query with the actual value from the in-code aggregation

                    //merge the SQL with the current row values as parameters
                    const mergedSQL = this.mergeSQLWithParams(sql, row)                    
                    debug('mergedSQL:: ', mergedSQL)

                    let aggResult = await this.executeDistributed(aq.sql) //pass the row parameter to get the value to the columns that are in the `where` clause                
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

    }
}