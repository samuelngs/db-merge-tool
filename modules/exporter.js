
var fs    = require('fs'),
    
    async = require('async'),
    mysql = require('mysql'),
    mkdir = require('mkdirp'),
    colors = require('colors'),

    rimraf  = require('rimraf'),
    path    = require('path');

var emitter = new (require('events').EventEmitter);

var Exporter = function(arg) {
    if (!(this instanceof Exporter)) {
        return new Exporter(arg);
    }
    this.config = {};
    this.validConfig = false;
    this.connections = {
        primary: undefined,
        minor: undefined
    };
    this.successCount = 0;
    try {
        this.config = require(arg).database;
    } catch (e) {};
    this.validConfig = typeof this.config === 'object' && 
                  typeof this.config.primary === 'object' &&
                  typeof this.config.primary.db_host === 'string' &&
                  typeof this.config.primary.db_port === 'number' &&
                  typeof this.config.primary.db_name === 'string' &&
                  typeof this.config.primary.db_user === 'string' &&
                  typeof this.config.minor === 'object' &&
                  typeof this.config.minor.db_host === 'string' &&
                  typeof this.config.minor.db_port === 'number' &&
                  typeof this.config.minor.db_name === 'string' &&
                  typeof this.config.minor.db_user === 'string';
    if (!this.config.export) {
        this.config.export = 'backup';
    }
    if (!this.config.exclude) {
        this.config.exclude = [];
    }
    if (!this.validConfig) {
        console.error('# Configuration file is invalid') 
    }
    return this;
};

Exporter.prototype.connect = function() {
    if (!this.validConfig) return this;
    var self = this;
    async.each(Object.keys(this.connections), function(id, complete) {
        self.connections[id] = mysql.createConnection({
            host: self.config[id].db_host,
            port: self.config[id].db_port,
            user: self.config[id].db_user,
            password: self.config[id].db_pass,
            database: self.config[id].db_name
        });
        self.connections[id].connect(function(err) {
            if (!err) {
                self.successCount++
            }
            emitter.emit('connect', id, err ? err.stack : '# `' + id + '`	connected as id ' + self.connections[id].threadId, self.isConnected())
            complete();
        });
    });
    return this;
}

Exporter.prototype.clean = function(callback) {
    var self = this;
    emitter.emit('status', undefined, 'Removing mysql backup files');
    rimraf(path.join(__dirname, '/../' + self.config.export), function (err) {
        emitter.emit('status', undefined, err ? err.red : '# MySQL Backup Deleted'.yellow);
        if (typeof callback === 'function') callback.call(self);
    })
}

Exporter.prototype.dump = function() {
    if (!this.validConfig || !this.isConnected) return this;
    var self = this,
        tasks = {};
    async.each(Object.keys(this.connections), function(id, complete) {
        tasks[id] = function(done) {
            async.waterfall([
                function(next) {
                    emitter.emit('status', id, 'Start dumping mysql tables from `' + id + '`');
                    next();
                },
                function(next) {
                    self.connections[id].query('show tables;', function(err, results, fields) {
                        var tables = [];
                        async.each(results || [], function(item, callback) {
                            var keys = Object.keys(item);
                            if (keys.length == 1 && item[keys[0]] && self.config.exclude.indexOf(item[keys[0]]) == -1) {
                                tables.push(item[keys[0]]);
                            }
                            callback();
                        }, function(err) {
                            emitter.emit('status', id, '`' + id + '` database has ' + tables.length + ' tables');
                            next(undefined, tables);
                        })
                    });
                },
                function(tables, next) {
                    var target = __dirname + '/../' + self.config.export;
                    mkdir(target, function (err) {
                        next(undefined, tables);
                    });
                },
                function(tables, next) {
                    async.each(tables, function(table, callback) {
                        self.connections[id].query('select * from ' + table + ';', function(err, results, fields) {
                            emitter.emit('status', id, ('Exporting `' + table + '` table [data] from `' + id + '`...').blue);
                            if (!err) {
                                var folderPath = __dirname + '/../' + self.config.export + '/' + table;
                                emitter.emit('status', id, ('Saving `' + table + '` table [data] from `' + id + '`...').green);
                                mkdir(folderPath, function (err) {
                                    fs.writeFile(folderPath + '/' + id + '-table.json', JSON.stringify(results), function (err) {
                                        callback();
                                    });
                                });
                            } else {
                                emitter.emit('status', id, ('Fail to dump `' + table + '` table [data] from `' + id + '`!').red);
                                callback();
                            }
                        });
                    }, function(err) {
                        next(undefined, tables);
                    });
                },
                function(tables, next) {
                    async.each(tables, function(table, callback) {
                        self.connections[id].query('show create table ' + table + ';', function(err, results, fields) {
                            emitter.emit('status', id, ('Exporting `' + table + '` table [structure] from `' + id + '`...').blue);
                            if (!err) {
                                var folderPath = __dirname + '/../' + self.config.export + '/' + table;
                                emitter.emit('status', id, ('Saving `' + table + '` table [structure] from `' + id + '`...').green);
                                mkdir(folderPath, function (err) {
                                    fs.writeFile(folderPath + '/' + id + '-structure.sql', (results || [{}])[0]['Create Table'], function (err) {
                                        callback();
                                    });
                                });
                            } else {
                                emitter.emit('status', id, ('Fail to dump `' + table + '` table [structure] from `' + id + '`!').red);
                                callback();
                            }
                        });
                    }, function(err) {
                        next(undefined, tables);
                    });
                },
                function(tables, next) {
                    async.each(tables, function(table, callback) {
                        self.connections[id].query('show keys from ' + table + ' where Key_name = "PRIMARY";', function(err, results, fields) {
                            emitter.emit('status', id, ('Exporting `' + table + '` table [primary key] from `' + id + '`...').blue);
                            if (!err) {
                                var folderPath = __dirname + '/../' + self.config.export + '/' + table,
                                    row = (results || [])[0] || {},
                                    pid = row['Column_name'];
                                emitter.emit('status', id, ('Saving `' + table + '` table [primary key] from `' + id + '`...').green);
                                mkdir(folderPath, function (err) {
                                    fs.writeFile(folderPath + '/' + id + '-primary.txt', pid, function (err) {
                                        callback();
                                    });
                                });
                            } else {
                                emitter.emit('status', id, ('Fail to get `' + table + '` table [primary key] from `' + id + '`!').red);
                                callback();
                            }
                        });
                    }, function(err) {
                        next();
                    });
                }
            ], function(err) {
                done();
            });
        };
        complete();
    });
    async.parallel(tasks, function(err, results) {
        emitter.emit('end', ('[All tasks are completed]').cyan);
    });
}

Exporter.prototype.isConnected = function() {
    return Object.keys(this.connections).length == this.successCount;
}

Exporter.prototype.on = function(event, callback) {
    emitter.on(event, callback);
    return this;
}

Exporter.prototype.off = function(event, callback) {
    emitter.removeListener(event, callback);
    return this;
}

module.exports = exports = Exporter;