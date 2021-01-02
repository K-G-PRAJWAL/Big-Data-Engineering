var db = require('./db');

db.save({
  name: "Coffee", category: "beverages"
}, function  (err, res) {
  console.log(res);
});