let kafka = require('kafka-node');
let writeLog = require('./writeLog')
let queryString = require('query-string');
let useragent = require('useragent');
let CronJob = require('cron').CronJob;
let Async = require('async')

useragent(true)

var pageviewList = new Array()
var clickList = new Array()
var orderList = new Array()

var Consumer = kafka.Consumer,
// The client specifies the ip of the Kafka producer and uses
// the zookeeper port 2181
    client = new kafka.Client("localhost:2181"),
// The consumer object specifies the client and topic(s) it subscribes to
    consumer = new Consumer(client, [ { topic: "GrokkingLog", partition: 0 } ], { groupId: "group0", autoCommit: false });

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

    new CronJob('0 */5 * * * *', function() {
        console.log('You will see this message every minute');
        var taskList = []
        taskList.push(function (callback) {
            if (pageviewList.length > 0) {
                writeLog.writeDataPageView(pageviewList,function (error) {
                    if(error) callback(error)
                    else {
                        console.log('save pageviewList log success')
                    }
                })
                pageviewList = new Array()
                callback(null)
            } else {
                callback(null)
            }
        })
        
        taskList.push(function (callback) {
            if (clickList.length > 0) {
                writeLog.writeDataClick(clickList,function (error) {
                    if(error) callback(error)
                    else {
                        console.log('save clickList log success')
                    }
                })
                clickList = new Array()
                callback(null)
            } else {
                callback(null)
            }
        })
        taskList.push(function (callback) {
            if (orderList.length > 0) {
                writeLog.writeDataOrder(orderList,function (error) {
                    if(error) callback(error)
                    else {
                        console.log('save orderList log success')
                    }
                })
                orderList = new Array()
                callback(null)
            } else {
                callback(null)
            }
        })
        Async.series(taskList,function (error,results) {
            if (error) console.log(error)
            else {
                console.log('save data')
                // consumer.commit(function (err,data) {
                //     if (err) console.log(err)
                //     else {
                //         console.log('data:',data)
                //     }
                // })
            }
        })
    }, null, true, 'America/Los_Angeles');
});
