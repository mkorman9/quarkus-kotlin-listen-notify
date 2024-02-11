package com.github.mkorman9

data class SampleEvent(val payload: String) {
    companion object {
        const val EVENTBUS_ADDRESS = "sample_events"
    }
}
