
var fs    = require('fs'),

	async = require('async'),
	mysql = require('mysql'),
	colors = require('colors'),

	ProgressBar = require('progress'),

	_ = require('lodash');

var emitter = new (require('events').EventEmitter);

var Merge = function(arg) {
	if (!(this instanceof Merge)) {
		return new Merge(arg);
	}
	this.config = {};
	this.validConfig = false;
	this.connections = {
		dist: undefined
	};
	this.successCount = 0;
	this.imported = false;
	this.tables = {};
	try {
		this.config = require(arg).database;
	} catch (e) {};
	this.validConfig = typeof this.config === 'object' &&
				  typeof this.config.dist === 'object' &&
				  typeof this.config.dist.db_host === 'string' &&
				  typeof this.config.dist.db_port === 'number' &&
				  typeof this.config.dist.db_name === 'string' &&
				  typeof this.config.dist.db_user === 'string';
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
}

Merge.prototype.connect = function(err) {
	if (!this.validConfig) return;
	var self = this;
	async.each(Object.keys(this.connections), function(id, complete) {
		self.connections[id] = mysql.createConnection({
			host: self.config[id].db_host,
			port: self.config[id].db_port,
			user: self.config[id].db_user,
			password: self.config[id].db_pass
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

Merge.prototype.mysql = function(name) {
	return this.connections[name]
}

Merge.prototype.assign = function(callback) {
	var self = this,
		folderPath = __dirname + '/../' + this.config.export;
	async.waterfall([
		function(complete) {
			fs.readdir(folderPath, function(err, files) {
				var tables = [];
				async.each(files, function(file, next) {
					var filePath = folderPath + '/' + file;
					fs.lstat(filePath, function(err, stats) {
						if (stats.isDirectory() && self.config.exclude.indexOf(file) == -1) {
							tables.push(file)
						}
						next();
					});
				}, function(err) {
					complete(err, tables);
				})
			});
		},
		function(tables, complete) {
			var tasks = [];
			async.each(tables, function(table, next) {
				var task = self.generateTask.call(self, table, folderPath + '/' + table);
				tasks.push(task);
				next();
			}, function(err) {
				complete(undefined, tasks);
			})
		}
	], function(err, tasks) {
		if (typeof callback === 'function') callback.call(self, err, tasks);
	})
}

Merge.prototype.generateTask = function(tableName, path) {
	var self = this;
	return function(complete) {
		async.waterfall([
			function(next) {
				self.import(tableName, function(err) {
					next();
				});
			},
			function(next) {
				var cache = self.tables[tableName],
					primaryId = cache['primary_id'],
					primaryData = cache['primary'] || [],
					minorData = cache['minor'] || [];
				next(undefined, primaryId, primaryData, minorData);
			},
			function(primaryId, primaryData, minorData, next) {
				if (typeof primaryId === 'string' &&
					primaryData.length > 0 &&
					typeof primaryData[0][primaryId] === 'number' &&
					minorData.length > 0 &&
					typeof minorData[0][primaryId] === 'number') {
					emitter.emit('status', ('Sorting table `' + tableName + '`...').yellow);
					primaryData = self.quickSort(primaryData, primaryId);
					minorData   = self.quickSort(minorData, primaryId);
					emitter.emit('status', ('Remove duplicates from `' + tableName + '`...').yellow);
					primaryData = _.uniq(primaryData, true, primaryId);
					minorData = _.uniq(minorData, true, primaryId);
				}
				next(undefined, primaryId, primaryData, minorData);
			},
			function(primaryId, primaryData, minorData, next) {
				var arr = [],
					maxRowToCombine = 65000;
                if (typeof primaryId === 'string' &&
					primaryData.length > 0 &&
					typeof primaryData[0][primaryId] === 'number' &&
					minorData.length > 0 &&
					typeof minorData[0][primaryId] === 'number' &&
					primaryData.length < maxRowToCombine &&
					minorData.length < maxRowToCombine) {
					emitter.emit('status', ('Merging table `' + tableName + '` [Combine Method]... (' + (primaryData.length > minorData.length ? primaryData.length : minorData.length) + ' Records)').blue);
					arr = self.combine(minorData, primaryData, primaryId);
					next(undefined, arr);
                } else {
                	if (primaryData.length > minorData.length) {
						emitter.emit('status', ('Merging table `' + tableName + '` [Add ' + (primaryData.length - minorData.length) + ' missing rows from primary]... (' + primaryData.length + ' Records)').blue);
                		arr = minorData.concat(_.takeRight(primaryData, primaryData.length - minorData.length));
                	} else if (minorData.length < primaryData.length) {
						emitter.emit('status', ('Merging table `' + tableName + '` [Add ' + (minorData.length - primaryData.length) + ' missing rows from minor]... (' + minorData.length + ' Records)').blue);
                		arr = primaryData.concat(_.takeRight(minorData, minorData.length - primaryData.length));
                	} else {
						emitter.emit('status', ('Merging table `' + tableName + '` [Use Primary]...').blue);
                		arr = primaryData;
                	}
                	next(undefined, arr);
                }
			},
			function(data, next) {
				var createCmd = self.tables[tableName].create;
				self.mysql('dist').query(createCmd, function(err, results, fields) {
                	emitter.emit('status', err ? ('Failed to create table `' + tableName + '`: ' + err).red : ('Created table `' + tableName + '`...').yellow);
					next(err, data);
				});
			},
			function(data, next) {
				var refs = [];
				var parts = [];
				var maxPerInsert = 1000;
				async.each(data, function(row, done) {
					var cols = [];
					var arr = [];
					if (refs.length == 0) {
						for (var prop in row) {
							refs.push(prop);
						}
					}
					for (var prop in row) {
						if (refs.indexOf(prop) > -1) {
							arr.push(row[prop]);
						}
					}
					if (parts.length == 0 || (parts.length > 0 && parts[parts.length - 1].length >= maxPerInsert)) {
						parts.push([]);
					}
					parts[parts.length - 1].push(arr);
					done();
				}, function(err) {
					next(undefined, refs, parts);
				});
			},
			function(refs, parts, next) {
				var cols = refs.map(function(col) {
					return '`' + col + '`'
				}).join(', ');
				var idx = 0;
				async.each(parts, function(row, done) {
					var cmd = 'REPLACE INTO `' + tableName + '` (' + cols + ') VALUES ?';
					self.mysql('dist').query(cmd, [row], function(err, results, fields) {
						if (err) {
							emitter.emit('status', ('[' + (idx + 1) + '] Failed to insert ' + row.length + ' rows to table `' + tableName + '`: ' + err).red);
						} else {
							emitter.emit('status', ('[' + (idx + 1) + '] Insert ' + row.length + ' rows to table `' + tableName + '` successfully').yellow);
						}
						idx++;
						done();
					});
				}, function(err) {
					next(err);
				})
			},
			function(next) {
				delete self.tables[tableName];
				next();
			}
		], function(err) {
			complete()
		})
	}
}

Merge.prototype.start = function() {
	var self = this;
	self.assign(function(err, tasks) {
		self.mysql('dist').query('DROP DATABASE IF EXISTS ' + self.config.dist.db_name, function(err, results, fields) {
			self.mysql('dist').query('CREATE DATABASE IF NOT EXISTS ' + self.config.dist.db_name, function(err, results, fields) {
				self.mysql('dist').changeUser({
					host: self.config.dist.db_host,
					port: self.config.dist.db_port,
					user: self.config.dist.db_user,
					password: self.config.dist.db_pass,
					database: self.config.dist.db_name
				}, function(err) {
					async.waterfall(tasks, function(err) {
						emitter.emit('end', '[All tasks are completed]'.cyan);
					});
				});
			});
		});
	});
}

Merge.prototype.import = function(tableName, callback) {
	var self = this,
		folderPath = __dirname + '/../' + self.config.export + '/' + tableName;
	async.waterfall([
		function(complete) {
			self.tables[tableName] || (self.tables[tableName] = {});
			complete();
		},
		function(complete) {
			async.each(['primary', 'minor'], function(id, next) {
				fs.readFile(folderPath + '/' + id + '-table.json', 'utf-8', function (err, data) {
					self.tables[tableName][id] = JSON.parse(data || '[]');
					next(err);
				});
			}, function(err) {
				complete(err);
			});
		},
		function(complete) {
			fs.readFile(folderPath + '/primary-structure.sql', 'utf-8', function (err, data) {
				self.tables[tableName]['create'] = data || '';
				complete(err);
			});
		},
		function(complete) {
			fs.readFile(folderPath + '/primary-primary.txt', 'utf-8', function (err, data) {
				self.tables[tableName]['primary_id'] = (data || '').trim() || undefined;
				complete(err);
			});
		}
	], function(err) {
		emitter.emit('import', tableName, err);
		if (typeof callback === 'function') callback.call(self, err);
	})
}

Merge.prototype.table = function(name) {
	return this.tables[name];
}

Merge.prototype.combine = function(arr1, arr2, prop) {
	var arr3 = [];
	var bar = new ProgressBar('progress [:bar] :percent :etas', { 
		total: arr1.length, 
		complete: '=',
    	incomplete: ' ',
    	width: 40 
    });
	for(var i in arr1){
	   var shared = false;
	   for (var j in arr2)
	       if (arr2[j][prop] == arr1[i][prop]) {
	           shared = true;
	           break;
	       }
	   if(!shared) arr3.push(arr1[i])
	   bar.tick();
	}
	arr3 = arr3.concat(arr2);
	if (bar.complete) {
		emitter.emit('status', ('[Complete]').green);
	}
	return arr3;
}

Merge.prototype.quickSort = function(arr, key) {
	if (arr.length <= 1) {
		return arr;
	}
	var lessThan = [],
	    greaterThan = [],
	    pivotIndex = Math.floor(arr.length / 2),
	    pivot = arr.splice(pivotIndex, 1)[0];
	for (var i = 0, len = arr.length; i < len; i++) {
		if ((arr[i][key] < pivot[key]) || (arr[i][key] == pivot[key] && i < pivotIndex)) {
			lessThan.push(arr[i]);
		} else {
			greaterThan.push(arr[i]);
		}
    }
    return this.quickSort(lessThan, key).concat([pivot], this.quickSort(greaterThan, key));
}

Merge.prototype.isConnected = function() {
	return Object.keys(this.connections).length == this.successCount;
}

Merge.prototype.on = function(event, callback) {
	emitter.on(event, callback);
	return this;
}

Merge.prototype.off = function(event, callback) {
	emitter.removeListener(event, callback);
	return this;
}

module.exports = exports = Merge;
