package com.github.mkorman9

import io.quarkus.vertx.ConsumeEvent
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class SampleEventsListener {
    @ConsumeEvent(SampleEvent.QUEUE_NAME)
    fun onEvent(event: SampleEvent) {
        println("Sample event: ${event.payload}")
    }
}
