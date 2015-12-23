var program = require('commander'),
    colors  = require('colors');

var Exporter = require(__dirname + '/modules/exporter');
    Merge    = require(__dirname + '/modules/merge');

program
    .version('0.0.1')
    .usage('[options] <file ...>')
    .option('-d, --dump [config-file]', 'Dump MySQL databases')
    .option('-m, --merge [config file]', 'Merge databases')
    .parse(process.argv);

if (program.dump) {
    var exporter = new Exporter(__dirname + '/' + (typeof program.dump === 'string' ? program.dump : 'config.json'));
    exporter.on('connect', function(id, msg, connected) {
        console.log(msg);
        if (connected) exporter.clean(exporter.dump)
    }).on('status', function(id, msg) {
        console.log('[status]', msg);
    }).on('start', function(id, msg) {
        console.log(id)
    }).on('end', function(msg) {
        console.log(msg)
        process.exit();
    }).connect();
}

if (program.merge) {
    var merge = new Merge(__dirname + '/' + (typeof program.merge === 'string' ? program.merge : 'config.json'));
    merge.on('connect', function(id, msg, connected) {
        console.log(msg);
        if (connected) merge.start(); 
    }).on('import', function(table, err) {
        console.log('[%s] Import Table `%s` %s', err ? 'FAIL'.red : 'SUCCESS'.green, table, err ? err : '')
    }).on('status', function(msg) {
        console.log(msg)
    }).on('start', function(table) {
        console.log('Start importing rows to table `%s`', table);
    }).on('end', function(msg) {
        console.log(msg)
        process.exit();
    }).connect();
}

if (!process.argv.slice(2).length) {
    program.outputHelp();
}
