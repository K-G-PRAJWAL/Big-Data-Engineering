var db = require('./db');

db.get('6b6a6fb26d874827c7b00f32ff0014f8', function  (err, doc) {
  console.log(doc);
});