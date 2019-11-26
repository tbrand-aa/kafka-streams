
module.exports = {
  noptions: { // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#global-configuration-properties
    'metadata.broker.list': 'localhost:9092',
    'group.id': 'example-group',
    'enable.auto.offset.store': false,
    
    offset_commit_cb (err, topicPartitions) {
      if (err) {
        return console.error(err)
      }

      console.log('OFFSET_COMMITTED: ', topicPartitions)
    },
  },
  tconf: { // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties
    // 'auto.offset.reset': 'earliest',
    // 'request.required.acks': 1,
    'consume.callback.max.messages': 1,
  },
  batchOptions: {
    "batchSize": 1,
    "commitEveryNBatch": 1,
    "concurrency": 1,
    "commitSync": true,
    "noBatchCommits": false
  }
}
