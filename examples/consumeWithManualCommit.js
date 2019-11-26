'use strict'

const { KafkaStreams } = require('./../index')

const kafkaStreams = new KafkaStreams(require('./exampleConfig'))
const kafkaStream = kafkaStreams.getKStream()


kafkaStream
  .from('TEST_IN')
  .asyncMap(map)
  .commitAfterTo()
  .to('TEST_OUT')

kafkaStream.start(() => {
  console.log('Kafka stream ready!')
}, (err) => {
  console.error('Kafka ERROR!', err)
}, true)

async function map ({message, done}) {
  // console.log('message: ', message);
  console.log(`Receiving message. offset= ${message.offset}`)

  // Force wait
  await sleep(2000)
  console.log(`Handled message.   offset= ${message.offset}`)
  
  
  return JSON.stringify({
    value: Buffer.from(message.value).toString(),
    offset: message.offset
  })
}

const sleep = ms => new Promise((resolve, reject) => setTimeout(resolve, ms))
