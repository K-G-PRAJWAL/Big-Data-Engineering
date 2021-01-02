var cradle = require('cradle');

var db = new(cradle.Connection)('http://127.0.0.1', 5984, {
  auth: {username: 'admin', password: 'admin'}
}).database('restaurant');

module.exports = db;