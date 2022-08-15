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

package com.exactpro.th2.read.db.bootstrap

import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureNanoTime

class Saver<K, V>(
    private val queue: BlockingQueue<V>,
    private val running: AtomicBoolean,
    private val maxBatchSize: Int,
    private val maxDelayInQueueMillis: Long,
    private val keyExtractor: (V) -> K,
    private val preprocessing: (K, V) -> V,
    private val onResult: (List<V>) -> Unit,
) : Runnable {

    override fun run() = try {
        val groups = hashMapOf<K, Group<K, V>>()
        LOGGER.info { "Start polling events from the queue" }
        try {
            while (running.get() && !Thread.currentThread().isInterrupted) {
                LOGGER.trace { "Checking for new events. Queue size: ${queue.size}, Groups: ${groups.size}" }
                do {
                    val data = queue.poll(100, TimeUnit.MILLISECONDS)
                    if (data == null) {
                        checkMaxTimeout(groups)
                        break
                    }
                    val key = keyExtractor(data)
                    val group = groups.computeIfAbsent(key, ::Group)
                    group.add(preprocessing(key, data))
                    if (group.size >= maxBatchSize) {
                        group.store(onResult)
                    }
                    checkMaxTimeout(groups)
                } while (queue.isNotEmpty())
            }
            groups.values.forEach { it.store(onResult) }
            groups.clear()
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Exception during events polling" }
        }
        LOGGER.info { "Stop polling events from the queue. Queue size: ${queue.size}" }
    } catch (ex: Exception) {
        LOGGER.error(ex) { "error during pulling events queue" }
    }

    private class Group<K, V>(
        private val key: K,
    ) {
        private val data: MutableList<V> = arrayListOf()
        var lastPublicationTime: Long = current()

        val size: Int
            get() = data.size

        fun add(event: V) {
            data += event
        }

        fun isTimeToPublish(delay: Long): Boolean = (lastPublicationTime + delay) < current() && data.isNotEmpty()

        fun store(onData: (List<V>) -> Unit) {
            if (data.isEmpty()) {
                return
            }
            try {
                measureNanoTime {
                    runCatching {
                        onData(data)
                    }.onSuccess {
                        LOGGER.trace { "Data stored" }
                    }.onFailure {
                        LOGGER.error(it) { "Cannot store data: ${data.size} elements" }
                    }
                }.also { LOGGER.debug { "Time to get response from event store: ${Duration.ofNanos(it)}" } }
            } catch (ex: Exception) {
                LOGGER.error(ex) { "Cannot store data" }
            }
            lastPublicationTime = current()
            data.clear()
        }

        private fun current() = System.currentTimeMillis()
    }

    private fun checkMaxTimeout(groups: HashMap<K, Group<K, V>>) {
        groups.values.asSequence()
            .filter { it.isTimeToPublish(maxDelayInQueueMillis) }
            .forEach {
                it.store(onResult)
            }
    }


    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}
