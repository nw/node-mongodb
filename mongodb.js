sys = require("sys");

Connection = require("./mongo").Connection

function Collection(mongo, db, name) {
    this.mongo = mongo;
    this.ns = db + "." + name;
    this.db = db;
    this.name = name;
}

Collection.prototype.find = function(query, fields) {
    var promise = new process.Promise;
    this.mongo.addQuery(promise, this.ns, query, fields);
    return promise;
}

Collection.prototype.find_one = function(query, fields, ns) {
    var promise = new process.Promise;
    var user_promise = new process.Promise;

    this.mongo.addQuery(promise, ns || this.ns, query, fields, 1);

    promise.addCallback(function (results) {
        // XXX what if result.Length < 1
        sys.puts("find_one callback " + JSON.stringify(results[0]));
        user_promise.emitSuccess(results[0]);
    });
    return user_promise;
}

Collection.prototype.count = function(query) {
    ns = this.db + ".$cmd";
    var cmd = {
        "count": this.name,
        "query": query
    }

    user_promise = new process.Promise;
    promise = this.find_one(cmd, {}, ns);
    promise.addCallback(function (result) {
        // check $err
        sys.puts("in count callback" + JSON.stringify(result));
        sys.puts(JSON.stringify(result));
        user_promise.emitSuccess(result.n);
    });

    return user_promise;
}

function MongoDB() {
    this.connection = new Connection;

    self = this;

    this.connection.addListener("ready", function () {
        self.dispatch();
    });

    this.connection.addListener("connection", function () {
        self.emit("connection");
    });

    this.connection.addListener("result", function(result) {
        var promise = self.currentQuery[0];
        self.currentQuery = null;
        promise.emitSuccess(result);
    });
}

sys.inherits(MongoDB, process.EventEmitter);

MongoDB.prototype.connect = function(args) {
    self = this;

    this.queries = [];
    this.hostname = args.hostname || "127.0.0.1";
    this.port = args.port || 27017;
    this.db = args.db;

    this.connection.connect(this.hostname, this.port);
}

MongoDB.prototype.addQuery = function(promise, ns, query, fields, limit, skip ) {
    var q = [ promise, ns ];
    if (query) q.push(query);
    if (fields) q.push(fields);
    if (limit) q.push(limit);
    if (skip) q.push(skip);
    this.queries.push(q);
}

MongoDB.prototype.dispatch = function() {
    if (this.currentQuery || !this.queries.length) return;

    this.currentQuery = this.queries.shift();
    this.connection.find.apply(this.connection, this.currentQuery.slice(1));
}

MongoDB.prototype.getCollection = function(name) {
    return new Collection(this, this.db, name);
}

exports.MongoDB = MongoDB;
