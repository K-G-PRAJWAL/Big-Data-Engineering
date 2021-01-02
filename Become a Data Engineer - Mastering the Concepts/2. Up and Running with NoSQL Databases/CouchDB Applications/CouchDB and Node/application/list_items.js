var db = require('./db');

db.view('menu/items', function  (err, res) {

  for (var i=0; i < res.length; i++) {
    var row = res[i];
    console.log(row);
  }

});