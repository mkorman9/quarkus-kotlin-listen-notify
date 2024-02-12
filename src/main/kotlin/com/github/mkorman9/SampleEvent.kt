package com.github.mkorman9

data class SampleEvent(val payload: String) {
    companion object {
        const val QUEUE_NAME = "sample_events"
    }
}
