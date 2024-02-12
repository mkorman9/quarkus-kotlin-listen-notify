package com.github.mkorman9.notifications

import com.github.mkorman9.SampleEvent
import kotlin.reflect.KClass

sealed class PostgresQueue<T : Any>(
    val name: String,
    val payloadClass: KClass<T>
) {
    data object SampleEvents : PostgresQueue<SampleEvent>(SampleEvent.QUEUE_NAME, SampleEvent::class)

    companion object {
        fun entries(): List<PostgresQueue<*>> {
            return PostgresQueue::class.sealedSubclasses
                .mapNotNull { it.objectInstance }
        }

        fun findByQueueName(queueName: String): PostgresQueue<*>? {
            return entries().find { queue -> queue.name == queueName }
        }
    }
}
