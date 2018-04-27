module.exports = {

    normalizeQuery: function(query) {
        return query.replace(/\s\s+/g, ' ').replace(/\n/g, '').toLowerCase().trim()
    },
    /**
     * Extract all the terms from projection and returns it as an array
     * @returns Array [] with the projections terms
     */
    extractProjectionTerms: function(query) {
        query = this.normalizeQuery(query)
        return query.substring(query.indexOf('select') + 'select'.length, query.indexOf('from'))
                              .split(',')
                              .map(p => p.trim())
    }
}