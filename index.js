var kafka = require('kafka-node');
var Consumer = kafka.Consumer,
// The client specifies the ip of the Kafka producer and uses
// the zookeeper port 2181
    client = new kafka.Client('localhost:2181'),
// The consumer object specifies the client and topic(s) it subscribes to
    consumer = new Consumer(client, [ { topic: 'GrokkingLog'} ], { groupId: 'group0', autoCommit: true });

consumer.on('message', function (message) {
    // grab the main content from the Kafka message
    var data = JSON.parse(message.value);
    console.log(data);
});