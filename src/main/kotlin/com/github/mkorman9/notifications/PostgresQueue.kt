package com.github.mkorman9.notifications

import com.github.mkorman9.SampleEvent
import kotlin.reflect.KClass

enum class PostgresQueue(
    val eventBusAddress: String,
    val payloadClass: KClass<out Any>
) {
    SAMPLE_EVENTS(SampleEvent.EVENTBUS_ADDRESS, SampleEvent::class)
}
