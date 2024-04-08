/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:JvmName("Main")
package com.exactpro.th2.read.db.bootstrap

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.factory.extensions.getCustomConfiguration
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.common.utils.shutdownGracefully
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.lwdataprovider.MessageSearcher
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.DataBaseReaderConfiguration
import com.exactpro.th2.read.db.app.validate
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.impl.grpc.DataBaseReaderGrpcServer
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.Deque
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.system.exitProcess
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.MessageGroupBatch as ProtoMessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) {
    LOGGER.info { "Starting the read-db service" }
    // Here is an entry point to the th2-box.

    // Configure shutdown hook for closing all resources
    // and the lock condition to await termination.
    //
    // If you use the logic that doesn't require additional threads,
    // and you can run everything on main thread
    // you can omit the part with locks (but please keep the resources queue)
    val resourceRegister = ResourceRegister()
    val lock = ReentrantLock()
    val condition: Condition = lock.newCondition()
    configureShutdownHook(resourceRegister, lock, condition)

    try {
        // You need to initialize the CommonFactory
        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resourceRegister.add("common factory", factory::close)

        setupApp(factory, resourceRegister::add)

        awaitShutdown(lock, condition)
    } catch (ex: Exception) {
        LOGGER.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}

class ResourceRegister: AutoCloseable {
    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

    fun add(name: String, resource: () -> Unit) {
        resources += {
            LOGGER.info { "Closing resource $name" }
            runCatching(resource).onFailure {
                LOGGER.error(it) { "cannot close resource $name" }
            }
        }
    }

    override fun close() {
        resources.descendingIterator().forEachRemaining { resource ->
            try {
                resource()
            } catch (e: Exception) {
                LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
    }

}

internal fun setupApp(
    factory: CommonFactory,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
) {
    val cfg = factory.getCustomConfiguration<DataBaseReaderConfiguration>()
    val errors: List<String> = cfg.validate()
    if (errors.isNotEmpty()) {
        LOGGER.error { "Configuration errors found:" }
        errors.forEach { LOGGER.error(it) }
        throw IllegalArgumentException("Invalid configuration. ${errors.size} error(s) found")
    }

    // The BOX is alive
    LIVENESS_MONITOR.enable()

    val appScope = createScope(closeResource)
    val componentBookName = factory.boxConfiguration.bookName
    val rootEventId = factory.rootEventId
    val maxEventBatchSize = factory.cradleManager.storage.entitiesFactory.maxTestEventBatchSize

    val messageLoader: MessageLoader = createMessageLoader(factory, componentBookName)
    val reader = if (cfg.useTransport) {
        val messageRouter: MessageRouter<GroupBatch> = factory.transportGroupBatchRouter
        val messageQueue: BlockingQueue<RawMessage.Builder> = configureTransportMessageStoring(
            cfg,
            ::transportKeyExtractor,
            TransportPreprocessor(componentBookName),
            closeResource
        ) {
            val sessionAlias = it[0].idBuilder().sessionAlias
            messageRouter.sendAll(
                GroupBatch(
                    componentBookName,
                    sessionAlias,
                    it.map {  builder -> builder.build().toGroup() }
                ),
                QueueAttribute.TRANSPORT_GROUP.value
            )
        }
        createReader(cfg, appScope, messageQueue, closeResource, TableRow::toTransportMessage, messageLoader)
    } else {
        val messageRouter = factory.messageRouterMessageGroupBatch
        val messageQueue = configureTransportMessageStoring(
            cfg,
            ::protoKeyExtractor,
            ProtoPreprocessor(componentBookName),
            closeResource
        ) {
            messageRouter.sendAll(
                ProtoMessageGroupBatch.newBuilder()
                    .apply {
                        it.forEach {
                            addGroupsBuilder() += it
                        }
                    }
                    .build(),
                QueueAttribute.RAW.value,
            )
        }
        createReader(cfg, appScope, messageQueue, closeResource, TableRow::toProtoMessage, messageLoader)
    }

    val eventBatcher = configureEventStoring(cfg, maxEventBatchSize, closeResource, factory.eventBatchRouter::send)

    val handler = DataBaseReaderGrpcServer(
        reader,
        { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
        { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
    ) { event, parentEventId ->
        eventBatcher.onEvent(
            event.toProto(parentEventId ?: rootEventId)
        )
    }

    val server = factory.grpcRouter.startServer(handler)
        .start()
    closeResource("grpc-server") {
        LOGGER.info { "Shutting down gRPC server" }
        val unit = TimeUnit.MINUTES
        val timeout: Long = 1
        if (server.shutdown().awaitTermination(timeout, unit)) {
            LOGGER.warn { "Cannot shutdown server in ${unit.toMillis(timeout)} millis. Shutdown now" }
            server.shutdownNow()
        }
    }

    // The BOX is ready to work
    READINESS_MONITOR.enable()
}

private fun createMessageLoader(
    factory: CommonFactory,
    componentBookName: String
) = runCatching {
    MessageSearcher.create(factory.grpcRouter.getService(DataProviderService::class.java)).run {
        MessageLoader { dataSourceId, horizon, properties ->
            val horizonTimestamp: Timestamp? = horizon?.toTimestamp()
            val searcherInterval: Duration = horizon?.let {
                // FIXME: add overload with explicit end time for findLastOrNull method
                Duration.between(it, Instant.now()).plusHours(1)
            } ?: Duration.ofDays(1)

            findLastOrNull(
                book = componentBookName,
                sessionAlias = dataSourceId.id,
                direction = FIRST,
                searchInterval = searcherInterval,
            ) {
                with(it.message) {
                    (horizon?.let { Timestamps.compare(horizonTimestamp, messageId.timestamp) < 0 } ?: true)
                            && properties.all { (key, value) -> getMessagePropertiesOrDefault(key, null) == value }
                }
            }?.toTableRow()
        }
    }
}.onFailure {
    LOGGER.warn(it) {
        "Loading message from a data-provider is disabled because gRPC pin for ${DataProviderService::class.java} service isn't configured"
    }
}.onSuccess {
    LOGGER.info { "Loading message from a data-provider is enabled" }
}.getOrNull() ?: MessageLoader.DISABLED

private fun <BUILDER: Any> createReader(
    cfg: DataBaseReaderConfiguration,
    appScope: CoroutineScope,
    messageQueue: BlockingQueue<BUILDER>,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
    toMessage: TableRow.(DataSourceId, Map<String, String>) -> BUILDER,
    loadLastMessage: MessageLoader,
): DataBaseReader {
    val reader = DataBaseReader.createDataBaseReader(
        cfg,
        appScope,
        pullingListener = object : UpdateListener {
            override fun onUpdate(dataSourceId: DataSourceId, row: TableRow, properties: Map<String, String>) {
                messageQueue.put(row.toMessage(dataSourceId, properties))
            }

            override fun onError(dataSourceId: DataSourceId, reason: Throwable) {
                LOGGER.error(reason) { "error during pulling updates from $dataSourceId" }
            }

            override fun onComplete(dataSourceId: DataSourceId) {
                LOGGER.info { "Pulling updates from $dataSourceId completed" }
            }

        },
        rowListener = { sourceId, row ->
            LOGGER.debug { "Storing row from $sourceId. Columns: ${row.columns.keys}" }
            messageQueue.put(row.toMessage(sourceId, emptyMap()))
        },
        messageLoader = loadLastMessage
    )
    closeResource("reader", reader::close)
    reader.start()
    return reader
}

private fun createScope(closeResource: (name: String, resource: () -> Unit) -> Unit): CoroutineScope {
    val appScope = CoroutineScope(
        Dispatchers.IO
                + SupervisorJob() /*because we need keep the main coroutine running*/
                + CoroutineName("app-coroutine")
    )
    closeResource("coroutine scope", appScope::cancel)
    return appScope
}

private fun configureEventStoring(
    cfg: DataBaseReaderConfiguration,
    maxEventBatchSize: Int,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
    send: (EventBatch) -> Unit
): EventBatcher {
    val executor = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("event-saver-%d")
            .build()
    )

    closeResource("event storing") {
        LOGGER.info { "Shutdown executor" }
        executor.shutdownGracefully(1, TimeUnit.MINUTES)
    }

    return EventBatcher(
        maxEventBatchSize.toLong(),
        cfg.eventPublication.maxBatchSizeInItems,
        cfg.eventPublication.maxFlushTime,
        executor,
        send
    )
}

private fun <BUILDER, DIRECTION> configureTransportMessageStoring(
    cfg: DataBaseReaderConfiguration,
    keyExtractor: (BUILDER) -> SessionKey<DIRECTION>,
    preprocessor: Preprocessor<BUILDER, DIRECTION>,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
    send: (List<BUILDER>) -> Unit
): BlockingQueue<BUILDER> {
    val messagesQueue: BlockingQueue<BUILDER> = ArrayBlockingQueue(cfg.publication.queueSize)

    val executor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("transport-message-saver-%d")
            .build()
    )

    val running = AtomicBoolean(true)
    val drainFuture = executor.submit(Saver(
        messagesQueue,
        running,
        cfg.publication.maxBatchSize,
        cfg.publication.maxDelayMillis,
        keyExtractor,
        preprocessor::preprocess,
        send
    ))

    closeResource("transport message storing") {
        if (running.compareAndSet(true, false)) {
            try {
                drainFuture.get(1, TimeUnit.MINUTES)
            } catch (ex: TimeoutException) {
                LOGGER.error(ex) { "cannot complete drain task in specified timeout" }
            }
            LOGGER.info { "Shutdown executor" }
            executor.shutdownGracefully(1, TimeUnit.MINUTES)
        }
    }
    return messagesQueue
}

private val nanosInSecond = TimeUnit.SECONDS.toNanos(1)

private fun protoKeyExtractor(builder: ProtoRawMessage.Builder): SessionKey<ProtoDirection> =
    SessionKey(builder.sessionAlias, builder.direction)

private fun transportKeyExtractor(builder: RawMessage.Builder): SessionKey<Direction> =
    SessionKey(builder.idBuilder().sessionAlias, builder.idBuilder().direction)

private abstract class Preprocessor<BUILDER, DIRECTION>(protected val configBookName: String) {
    protected val sequences = ConcurrentHashMap<SessionKey<DIRECTION>, Long>()
    abstract fun preprocess(key: SessionKey<DIRECTION>, builder: BUILDER): BUILDER
}

private class ProtoPreprocessor(bookName: String) : Preprocessor<ProtoRawMessage.Builder, ProtoDirection>(bookName) {
    override fun preprocess(key: SessionKey<ProtoDirection>, builder: ProtoRawMessage.Builder): ProtoRawMessage.Builder =
        builder.apply {
            sequence = sequences.compute(key) { _, prev ->
                if (prev == null) {
                    Instant.now().run { epochSecond * nanosInSecond + nano }
                } else {
                    prev + 1
                }
            }.let(::requireNotNull)
            metadataBuilder.idBuilder.apply {
                timestamp = Instant.now().toTimestamp()
                sessionGroup = sessionAlias
                bookName = configBookName
            }
        }
}

private class TransportPreprocessor(bookName: String) : Preprocessor<RawMessage.Builder, Direction>(bookName) {
    override fun preprocess(key: SessionKey<Direction>, builder: RawMessage.Builder): RawMessage.Builder {
        builder.idBuilder().apply {
            setSequence(requireNotNull(
                sequences.compute(key) { _, prev ->
                    if (prev == null) {
                        Instant.now().run { epochSecond * nanosInSecond + nano }
                    } else {
                        prev + 1
                    }
                }
            ))
            setTimestamp(Instant.now())
        }
        return builder
    }
}

private data class SessionKey<DIRECTION>(val alias: String, val direction: DIRECTION)

private fun configureShutdownHook(resourceRegister: ResourceRegister, lock: ReentrantLock, condition: Condition) {
    Runtime.getRuntime().addShutdownHook(thread(
        start = false,
        name = "Shutdown hook"
    ) {
        LOGGER.info { "Shutdown start" }
        READINESS_MONITOR.disable()
        try {
            lock.lock()
            condition.signalAll()
        } finally {
            lock.unlock()
        }
        resourceRegister.close()
        LIVENESS_MONITOR.disable()
        LOGGER.info { "Shutdown end" }
    })
}

@Throws(InterruptedException::class)
private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
    try {
        lock.lock()
        LOGGER.info { "Wait shutdown" }
        condition.await()
        LOGGER.info { "App shutdown" }
    } finally {
        lock.unlock()
    }
}