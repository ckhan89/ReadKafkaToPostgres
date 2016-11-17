let kafka = require('kafka-node');
let writeLog = require('./writeLog')
let queryString = require('query-string');
var logList = []
var Consumer = kafka.Consumer,
// The client specifies the ip of the Kafka producer and uses
// the zookeeper port 2181
    client = new kafka.Client("localhost:2181"),
// The consumer object specifies the client and topic(s) it subscribes to
    consumer = new Consumer(client, [ { topic: "GrokkingLog", partition: 0 } ], { groupId: "group0", autoCommit: true });

consumer.on('message', function (message) {
    // grab the main content from the Kafka message
    var params = queryString.parse(message.value)

    logList.push({'uuid':params.uuid, 'location': params.location, 'referrer': params.referrer, 'url': params.url,
                    'product': params.product, 'video': params.video, 'viewer': params.viewer})
    if (logList.length > 100000) {
        writeLog.writeData(logList,function (error) {
            if(error) console.log(error)
            else {
                console.log('save log success')
            }
        })
        logList = []
    }
});
