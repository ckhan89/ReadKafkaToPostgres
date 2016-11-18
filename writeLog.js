let pgp = require('pg-promise')();
var cn = {
    database: 'prod', //env var: PGDATABASE
    host: '61.28.227.191', // Server hosting the postgres database
    port: 5433, //env var: PGPORT
    user: 'postgres',
    password: 'ckhan89',
    max: 10, // max number of clients in the pool
    idleTimeoutMillis: 1, // how long a client is allowed to remain idle before being closed
};
var db = pgp(cn)

function Inserts(template, data) {
    if (!(this instanceof Inserts)) {
        return new Inserts(template, data);
    }
    this._rawDBType = true;
    this.formatDBType = function () {
        return data.map(d=>'(' + pgp.as.format(template, d) + ')').join();
    };
}

module.exports.writeDataPageView = function (jsonArray, callback) {
    var values = new Inserts('${uuid},${metric},${location},${referrer},${url},${product},${video},${viewer},${created_date}', jsonArray)
    let qformat = 'INSERT INTO pageview VALUES $1'
    db.none(qformat,values)
        .then(data=>{
            callback(null)
        })
        .catch(error=>{
            callback(error)
        })
}

module.exports.writeDataClick = function (jsonArray, callback) {
    var values = new Inserts('${metric},${uuid},${location},${referrer},${url},${product},${video},${viewer},${created_date}', jsonArray)
    let qformat = 'INSERT INTO click VALUES $1'
    db.none(qformat,values)
        .then(data=>{
            callback(null)
        })
        .catch(error=>{
            callback(error)
        })
}

module.exports.writeDataClick = function (jsonArray, callback) {
    var values = new Inserts('${metric},${uuid},${location},${referrer},${url},${product},${video},${order},$(customer),${created_date}', jsonArray)
    let qformat = 'INSERT INTO order_log VALUES $1'
    db.none(qformat,values)
        .then(data=>{
            callback(null)
        })
        .catch(error=>{
            callback(error)
        })
}