function(doc) {
  if(doc._attachments) {
    var filename;
    for (var key in doc._attachments) {
      if (doc._attachments.hasOwnProperty(key) && typeof(key) !== 'function') {
       filename = key;
       break;
      }
    }

    emit(doc.name, filename);
  }
}