/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.factory.extensions.getCustomConfiguration
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.DataBaseReaderConfiguration
import com.exactpro.th2.read.db.app.validate
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.impl.grpc.DataBaseReaderGrpcServer
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.protobuf.UnsafeByteOperations
import com.opencsv.CSVWriterBuilder
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
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

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) {
    LOGGER.info { "Starting the read-db service" }
    // Here is an entry point to the th2-box.

    // Configure shutdown hook for closing all resources
    // and the lock condition to await termination.
    //
    // If you use the logic that doesn't require additional threads
    // and you can run everything on main thread
    // you can omit the part with locks (but please keep the resources queue)
    val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()
    val lock = ReentrantLock()
    val condition: Condition = lock.newCondition()
    configureShutdownHook(resources, lock, condition)

    try {
        // You need to initialize the CommonFactory
        val resourceHandler: (name: String, resource: () -> Unit) -> Unit = { name, resource ->
            resources += {
                LOGGER.info { "Closing resource $name" }
                runCatching(resource).onFailure {
                    LOGGER.error(it) { "cannot close resource $name" }
                }
            }
        }

        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resourceHandler("common factory", factory::close)

        setupApp(factory, resourceHandler)

        awaitShutdown(lock, condition)
    } catch (ex: Exception) {
        LOGGER.error(ex) { "Cannot start the box" }
        exitProcess(1)
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

    val messageQueue: BlockingQueue<RawMessage.Builder> = configureMessageStoring(factory, cfg, closeResource)

    val appScope = createScope(closeResource)

    val reader = createReader(cfg, appScope, messageQueue, closeResource)

    val handler = DataBaseReaderGrpcServer(reader)

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

private fun createReader(
    cfg: DataBaseReaderConfiguration,
    appScope: CoroutineScope,
    messageQueue: BlockingQueue<RawMessage.Builder>,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
): DataBaseReader {
    val reader = DataBaseReader.createDataBaseReader(
        cfg,
        appScope,
        pullingListener = object : UpdateListener {
            override fun onUpdate(dataSourceId: DataSourceId, row: TableRow) {
                messageQueue.put(row.toMessage(dataSourceId))
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
            messageQueue.put(row.toMessage(sourceId))
        },
    )
    closeResource("reader", reader::close)
    reader.start()
    return reader
}

private fun TableRow.toMessage(dataSourceId: DataSourceId): RawMessage.Builder {
    return RawMessage.newBuilder()
        .setBody(UnsafeByteOperations.unsafeWrap(toCsvBody()))
        .apply {
            sessionAlias = dataSourceId.id
            direction = Direction.FIRST
            associatedMessageType?.also {
                metadataBuilder.putProperties("th2.csv.override_message_type", it)
            }
        }
}

private fun TableRow.toCsvBody(): ByteArray {
    return ByteArrayOutputStream().also {
        CSVWriterBuilder(it.writer())
            .withSeparator(',')
            .build().use { writer ->
                val columnNames = columns.keys.toTypedArray()
                val values: Array<String?> = columnNames.map { name -> columns[name]?.toStringValue() }
                    .toTypedArray()
                writer.writeNext(columnNames)
                writer.writeNext(values)
            }
    }.toByteArray()
}

private fun Any.toStringValue(): String = when (this) {
    is BigDecimal -> toPlainString()
    else -> toString()
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

private fun configureMessageStoring(
    factory: CommonFactory,
    cfg: DataBaseReaderConfiguration,
    closeResource: (name: String, resource: () -> Unit) -> Unit,
): BlockingQueue<RawMessage.Builder> {
    val messageRouter: MessageRouter<MessageGroupBatch> = factory.messageRouterMessageGroupBatch

    val messagesQueue: BlockingQueue<RawMessage.Builder> = ArrayBlockingQueue(cfg.publication.queueSize)

    val executor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("message-saver-%d")
            .build()
    )

    val sequences = ConcurrentHashMap<SessionKey, Long>()

    val nanosInSecond = TimeUnit.SECONDS.toNanos(1)

    val running = AtomicBoolean(true)
    val drainFuture = executor.submit(Saver<SessionKey, RawMessage.Builder>(
        messagesQueue,
        running,
        cfg.publication.maxBatchSize,
        cfg.publication.maxDelayMillis,
        { it.run { SessionKey(sessionAlias, direction) } },
        { key, builder ->
            builder.apply {
                sequence = sequences.compute(key) { _, prev ->
                    if (prev == null) {
                        Instant.now().run { epochSecond * nanosInSecond + nano }
                    } else {
                        prev + 1
                    }
                }.let(::requireNotNull)

                metadataBuilder.timestamp = Instant.now().toTimestamp()
            }
        }
    ) { messages ->
        messageRouter.sendAll(
            MessageGroupBatch.newBuilder()
                .apply {
                    messages.forEach {
                        addGroupsBuilder() += it
                    }
                }
                .build(),
            QueueAttribute.RAW.value,
        )
    })

    closeResource("message storing") {
        if (running.compareAndSet(true, false)) {
            try {
                drainFuture.get(1, TimeUnit.MINUTES)
            } catch (ex: TimeoutException) {
                LOGGER.error(ex) { "cannot complete drain task in specified timeout" }
            }
            LOGGER.info { "Shutdown executor" }
            executor.shutdown()
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.error { "executor was not shutdown during specified timeout. Force shutdown" }
                val runnables = executor.shutdownNow()
                LOGGER.error { "${runnables.size} task(s) left" }
            }
        }
    }
    return messagesQueue
}

private data class SessionKey(val alias: String, val direction: Direction)

private fun configureShutdownHook(resources: Deque<() -> Unit>, lock: ReentrantLock, condition: Condition) {
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
        resources.descendingIterator().forEachRemaining { resource ->
            try {
                resource()
            } catch (e: Exception) {
                LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
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