package com.github.mkorman9

import com.github.mkorman9.notifications.PostgresQueue
import com.github.mkorman9.notifications.PostgresQueueSender
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import java.sql.SQLException
import java.time.Duration

private const val SHUTDOWN_TIMEOUT: Long = 2000
private const val INTERVAL: Long = 1000

@ApplicationScoped
class SendingThread(
    private val sender: PostgresQueueSender
): Runnable {
    private lateinit var thread: Thread
    private var isRunning = true

    fun onStartup(@Observes startupEvent: StartupEvent) {
        thread = Thread.ofVirtual()
            .name("queue-sender-thread")
            .start(this)
    }

    fun onShutdown(@Observes shutdownEvent: ShutdownEvent) {
        isRunning = false
        thread.join(Duration.ofMillis(SHUTDOWN_TIMEOUT))
    }

    override fun run() {
        while (isRunning) {
            Thread.sleep(INTERVAL)

            try {
                sender.send(PostgresQueue.SampleEvents, SampleEvent("Hello world!"))
            } catch (e: SQLException) {
                // ignore
            }
        }
    }
}
