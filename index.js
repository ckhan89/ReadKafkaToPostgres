let kafka = require('kafka-node');
let writeLog = require('./writeLog')
let queryString = require('query-string');
let useragent = require('useragent');
useragent(true)

var pageviewList = new Array()
var clickList = new Array()
var orderList = new Array()

var Consumer = kafka.Consumer,
// The client specifies the ip of the Kafka producer and uses
// the zookeeper port 2181
    client = new kafka.Client("localhost:2181"),
// The consumer object specifies the client and topic(s) it subscribes to
    consumer = new Consumer(client, [ { topic: "GrokkingLog", partition: 0 } ], { groupId: "group0", autoCommit: true });

consumer.on('message', function (message) {
    // grab the main content from the Kafka message
    var array = message.value.split('\t')
    console.log(array)
    if(array.length > 1) {
        var params = queryString.parse(array[array.length -2])
        var created_date
        if (array.length > 2){
            created_date = new Date(array[array.length - 3])
        }

        var obj = params
        if (array.length > 3) {
            var agent = useragent.parse(array[array.length - 4]);
            obj.device = agent.device.toString()
            console.log(obj.device)
        }
        obj['created_date'] = created_date
        obj['viewer'] = params.viewer
        if (params.metric == 'pageview') {
            pageviewList.push(obj)
        } else if (params.metric == 'click') {
            clickList.push(obj)
        } else if (params.metric == 'order') {
            orderList.push(obj)
        }
    }
    // logList.push({'uuid':params.uuid, 'location': params.location, 'referrer': params.referrer, 'url': params.url,
    //                 'product': params.product, 'video': params.video, 'viewer': params.viewer})
    if (pageviewList.length > 10) {
        writeLog.writeDataPageView(pageviewList,function (error) {
            if(error) console.log(error)
            else {
                console.log('save pageviewList log success')
            }
        })
        pageviewList = new Array()
    }

    if (clickList.length > 10) {
        writeLog.writeDataClick(clickList,function (error) {
            if(error) console.log(error)
            else {
                console.log('save clickList log success')
            }
        })
        clickList = new Array()
    }

    if (orderList.length > 0) {
        writeLog.writeDataOrder(orderList,function (error) {
            if(error) console.log(error)
            else {
                console.log('save orderList log success')
            }
        })
        orderList = new Array()
    }
});