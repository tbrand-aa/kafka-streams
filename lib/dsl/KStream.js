"use strict";

const Promise = require("bluebird");
const { async: createSubject } = require("most-subject");
const lodashClone = require("lodash.clone");
const lodashCloneDeep = require("lodash.clonedeep");

const StreamDSL = require("./StreamDSL.js");
const { Window } = require("../actions/index");
const { messageProduceHandle } = require("../messageProduceHandle.js");

const NOOP = () => { };

/**
 * change-log representation of a stream
 */
class KStream extends StreamDSL {

    /**
     * creates a changelog representation of a stream
     * join operations of kstream instances are synchronous
     * and return new instances immediately
     * @param {string} topicName
     * @param {KStorage} storage
     * @param {KafkaClient} kafka
     * @param {boolean} isClone
     */
    constructor(topicName, storage = null, kafka = null, isClone = false) {
        super(topicName, storage, kafka, isClone);

        this.started = false;

        //readability
        if (isClone) {
            this.started = true;
        }
    }

    /**
     * start kafka consumption
     * prepare production of messages if necessary
     * when called with zero or just a single callback argument
     * this function will return a promise and use the callback for errors
     * @param {function|Object} kafkaReadyCallback - can also be an object (config)
     * @param {function} kafkaErrorCallback
     * @param {boolean} withBackPressure
     * @param {Object} outputKafkaConfig
     */
    start(kafkaReadyCallback = null, kafkaErrorCallback = null, withBackPressure = false, outputKafkaConfig = null) {

        if (kafkaReadyCallback && typeof kafkaReadyCallback === "object" && arguments.length < 2) {
            return new Promise((resolve, reject) => {
                this._start(resolve, reject, kafkaReadyCallback.withBackPressure, kafkaReadyCallback.outputKafkaConfig);
            });
        }

        if (arguments.length < 2) {
            return new Promise((resolve, reject) => {
                this._start(resolve, reject, withBackPressure);
            });
        }

        return this._start(kafkaReadyCallback, kafkaErrorCallback, withBackPressure, outputKafkaConfig);
    }

    _start(kafkaReadyCallback = null, kafkaErrorCallback = null, withBackPressure = false, outputKafkaConfig = null) {

        if (this.started) {
            throw new Error("this KStream is already started.");
        }

        this.started = true;

        if (this.noTopicProvided && !this.produceAsTopic) {
            return kafkaReadyCallback();
        }

        let producerReady = false;
        let consumerReady = false;

        const onReady = (type) => {

            switch (type) {
                case "producer": producerReady = true; break;
                case "consumer": consumerReady = true; break;
            }

            //consumer && producer
            if (producerReady && consumerReady && kafkaReadyCallback) {
                kafkaReadyCallback();
            }

            //consumer only
            if (!this.produceAsTopic && consumerReady && kafkaReadyCallback) {
                kafkaReadyCallback();
            }

            //producer only
            if (this.produceAsTopic && producerReady && kafkaReadyCallback && !this.kafka.topic || !this.kafka.topic.length) {
                kafkaReadyCallback();
            }
        };

        //overwrite kafka topics
        this.kafka.overwriteTopics(this.topicName);

        this.kafka.on("message", msg => super.writeToStream(msg));
        this.kafka.start(() => { onReady("consumer"); }, kafkaErrorCallback || NOOP, this.produceAsTopic, withBackPressure);

        if (this.produceAsTopic) {

            this.kafka.setupProducer(this.outputTopicName, this.outputPartitionsCount, () => { onReady("producer"); },
                kafkaErrorCallback, outputKafkaConfig);

            super.forEach(message => {
                messageProduceHandle(
                    this.kafka,
                    message,
                    this.outputTopicName,
                    this.produceType,
                    this.produceCompressionType,
                    this.produceVersion,
                    kafkaErrorCallback
                ).then(async (pmv) => {
                    console.log('after messageProcedureHandle, offset=', this.kafka.consumer._lastMessage.offset)
                    
                    console.log('Getting committed offsets...');
                    const offsets1 = await this.kafka.consumer.getComittedOffsets();
                    console.log('getComittedOffsets() before commitMessage', offsets1)

                    console.log('Committing message...');
                    const messageCommitted = this.kafka.consumer.commitMessage(false, this.kafka.consumer._lastMessage)
                    console.log('Message committed:', messageCommitted)

                    console.log('Getting committed offsets...');
                    const offsets2 = await this.kafka.consumer.getComittedOffsets()
                    console.log('getComittedOffsets() after commitMessage', offsets2)

                    console.log('Consumer committing...');
                    const consumerCommitted = this.kafka.consumer.commit();
                    console.log('Consumer commited:', consumerCommitted)

                    console.log('Getting committed offsets...');
                    const offsets3 = await this.kafka.consumer.getComittedOffsets()
                    console.log('getComittedOffsets() after commit', offsets3)
                });
            });

        }
    }

    /**
     * Emits an output when both input sources have records with the same key.
     * s1$:{object} + s2$:{object} -> j$:{left: s1$object, right: s2$object}
     * @param {StreamDSL} stream
     * @param {string} key
     * @param {boolean} windowed
     * @param {function} combine
     * @returns {KStream}
     */
    innerJoin(stream, key = "key", windowed = false, combine = null) {

        let join$ = null;
        if (!windowed) {
            join$ = this._innerJoinNoWindow(stream, key, combine);
        } else {
            throw new Error("not implemented yet."); //TODO implement
        }

        return this._cloneWith(join$);
    }

    _innerJoinNoWindow(stream, key, combine) {

        const existingKeyFilter = (event) => {
            return !!event && typeof event === "object" && typeof event[key] !== "undefined";
        };

        const melt = (left, right) => {
            return {
                left,
                right
            };
        };

        const parent$ = super.multicast().stream$.filter(existingKeyFilter);
        const side$ = stream.multicast().stream$.filter(existingKeyFilter);
        return parent$.zip(combine || melt, side$);
    }

    /**
     * Emits an output for each record in either input source.
     * If only one source contains a key, the other is null
     * @param {StreamDSL} stream
     */
    outerJoin(stream) {
        throw new Error("not implemented yet."); //TODO implement
    }

    /**
     * Emits an output for each record in the left or primary input source.
     * If the other source does not have a value for a given key, it is set to null
     * @param {StreamDSL} stream
     */
    leftJoin(stream) {
        throw new Error("not implemented yet."); //TODO implement
    }

    /**
     * Emits an output for each record in any of the streams.
     * Acts as simple merge of both streams.
     * can be used with KStream or KTable instances
     * returns a NEW KStream instance
     * @param {StreamDSL} stream
     * @returns {KStream}
     */
    merge(stream) {

        if (!(stream instanceof StreamDSL)) {
            throw new Error("stream has to be an instance of KStream or KTable.");
        }

        // multicast prevents replays
        // create a new internal stream that merges both KStream.stream$s
        const newStream$ = this.stream$.multicast().merge(stream.stream$.multicast());
        return this._cloneWith(newStream$);
    }

    _cloneWith(newStream$) {

        const kafkaStreams = this._kafkaStreams;
        if (!kafkaStreams) {
            throw new Error("merging requires a kafka streams reference on the left-hand merger.");
        }

        const newStorage = kafkaStreams.getStorage();
        const newKafkaClient = kafkaStreams.getKafkaClient();

        const newInstance = new KStream(null, newStorage, newKafkaClient, true);
        newInstance.replaceInternalObservable(newStream$);
        newInstance._kafkaStreams = kafkaStreams;

        return newInstance;
    }

    /**
     * creates a new KStream instance from a given most.js
     * stream; the consume topic will be empty and therefore
     * no consumer will be build
     * @param {Object} most.js stream
     * @returns {KStream}
     */
    fromMost(stream$) {
        return this._cloneWith(stream$);
    }

    /**
     * as only joins and window operations return new stream instances
     * you might need a clone sometimes, which can be accomplished
     * using this function
     * @param {boolean} cloneEvents - if events in the stream should be cloned
     * @param {boolean} cloneDeep - if events in the stream should be cloned deeply
     * @returns {KStream}
     */
    clone(cloneEvents = false, cloneDeep = false) {

        let clone$ = this.stream$.multicast();

        if (cloneEvents) {
            clone$ = clone$.map((event) => {

                if (!cloneDeep) {
                    return lodashClone(event);
                }

                return lodashCloneDeep(event);
            });
        }

        return this._cloneWith(clone$);
    }

    /**
     * Splits a stream into multiple branches based on cloning
     * and filtering it depending on the passed predicates.
     * [ (message) => message.key.startsWith("A"),
     *   (message) => message.key.startsWith("B"),
     *   (message) => true ]
     * ---
     * [ streamA, streamB, streamTrue ]
     * @param {Array<Function>} preds
     * @returns {Array<KStream>}
     */
    branch(preds = []) {

        if (!Array.isArray(preds)) {
            throw new Error("branch predicates must be an array.");
        }

        return preds.map((pred) => {

            if (typeof pred !== "function") {
                throw new Error("branch predicates must be an array of functions: ", pred);
            }

            return this.clone(true, true).filter(pred);
        });
    }

    /**
     * builds a window'ed stream across all events of the current kstream
     * when the first event with an exceeding "to" is received (or the abort()
     * callback is called) the window closes and emits its "collected" values to the
     * returned kstream
     * from and to must be unix epoch timestamps in milliseconds (Date.now())
     * etl can be a function that should return the timestamp (event time) of
     * from within the message e.g. m -> m.payload.createdAt
     * if etl is not given, a timestamp of receiving will be used (processing time)
     * for each event
     * encapsulated refers to the result messages (defaults to true, they will be
     * encapsulated in an object: {time, value}
     * @param {number} from
     * @param {number} to
     * @param {function} etl
     * @param {boolean} encapsulated - if event should stay encapsulated {time, value}
     * @param {boolean} collect - if events should be collected first before publishing to result stream
     * @returns {{window: *, abort: abort, stream: *}}
     */
    window(from, to, etl = null, encapsulated = true, collect = true) {

        if (typeof from !== "number" || typeof to !== "number") {
            throw new Error("from & to should be unix epoch ms times.");
        }

        //use this.stream$ as base, but work on a new one
        let stream$ = null;
        if (!etl) {
            stream$ = this.stream$.timestamp();
        } else {
            stream$ = this.stream$.map(element => {
                return {
                    time: etl(element),
                    value: element
                };
            });
        }

        let aborted = false;
        const abort$ = createSubject();
        function abort() {

            if (aborted) {
                return;
            }

            aborted = true;
            abort$.next(null);
        }

        const window = new Window([], collect);
        const window$ = window.getStream();
        stream$
            .skipWhile(event => event.time < from)
            .takeWhile(event => event.time < to)
            .until(abort$)
            .tap(event => window.execute(event, encapsulated))
            .drain().then(_ => {
                window.writeToStream();
                window.flush();
            })
            .catch((error) => {
                window$.error(error);
            });

        return {
            window,
            abort,
            stream: this._cloneWith(window$)
        };
    }

    commitAfterTo() {

        return this
    }

    /**
     * closes the internal stream
     * and all kafka open connections
     * as well as KStorage connections
     * @returns {Promise.<boolean>}
     */
    close() {
        this.stream$ = this.stream$.take(0);
        this.stream$ = null;
        this.kafka.close();
        return this.storage.close();
    }
}

module.exports = KStream;
