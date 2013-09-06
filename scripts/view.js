function (doc, meta) {
  if (doc.link_id!=undefined) {
    //var date = doc.time.slice(0, 5);

    var key = [doc.link_id, doc.timestamp];
    var value = {
      congestion:doc.congestion,
      count:1,
      age:doc.timestamp
    };
    emit(key, value);
  }
}

function (key, values, rereduce) {
  var result = {congestion:0, count:0, age:0};

    var sum = 0;
    var count = 0;
    var max = 0;
    for (var i=0; i<values.length; ++i) {
      if (values[i].age > max) {
          max = values[i].age;
      }

      sum = sum + values[i].congestion * values[i].count;
      count = count + values[i].count;

    }

    result.congestion = sum / count;
    result.count = count;
    result.age = max;

    return result;

}
