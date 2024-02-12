package com.github.mkorman9.notifications

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.vertx.core.eventbus.EventBus
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.jboss.logging.Logger
import org.postgresql.jdbc.PgConnection
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import javax.sql.DataSource
import kotlin.math.min
import kotlin.math.pow

private const val SHUTDOWN_TIMEOUT: Long = 2000
private const val RECEIVE_TIMEOUT: Int = 250
private const val RECEIVE_ERRORS_THRESHOLD = 3
private const val CONNECTION_ACQUIRE_BACKOFF_BASE = 2
private const val CONNECTION_ACQUIRE_MAX_WAIT_SEC: Long = 32

@ApplicationScoped
class PostgresQueueListener(
    private val log: Logger,
    private val dataSource: DataSource,
    private val eventBus: EventBus,
    private val objectMapper: ObjectMapper
) : Runnable {
    private lateinit var thread: Thread
    private lateinit var connection: PgConnection
    private var isRunning = true
    private var receiveErrors = 0

    fun onStartup(@Observes startupEvent: StartupEvent) {
        reconnect()
        thread = Thread.ofVirtual()
            .name("postgres-queue-listener-thread")
            .start(this)
    }

    fun onShutdown(@Observes shutdownEvent: ShutdownEvent) {
        isRunning = false
        thread.join(Duration.ofMillis(SHUTDOWN_TIMEOUT))
    }

    override fun run() {
        while (isRunning) {
            val notifications = try {
                connection.getNotifications(RECEIVE_TIMEOUT) ?: continue
            } catch (e: SQLException) {
                receiveErrors++
                if (receiveErrors > RECEIVE_ERRORS_THRESHOLD) {
                    log.warn("Postgres notifications fetching has failed, reconnecting")
                    reconnect()
                    receiveErrors = 0
                } else {
                    log.warn("Postgres notifications fetching has failed: $receiveErrors/$RECEIVE_ERRORS_THRESHOLD")
                }

                continue
            }

            for (notification in notifications) {
                val queue = PostgresQueue.findByQueueName(notification.name)
                if (queue == null) {
                    log.warn("Notification for queue ${notification.name} could not be routed")
                    continue
                }

                val payload = try {
                    objectMapper.readValue(notification.parameter, queue.payloadClass.java)
                } catch (e: JacksonException) {
                    log.warn("Failed to decode postgres notification from queue $queue", e)
                    continue
                }

                eventBus.send(queue.name, payload)
            }
        }
    }

    private fun reconnect() {
        val c = acquireConnection()
        connection = c.unwrap(PgConnection::class.java)
    }

    private fun acquireConnection(): Connection {
        var i = 0
        while (isRunning) {
            try {
                val connection = dataSource.connection
                subscribeToQueues(connection)
                return connection
            } catch (e: SQLException) {
                val backoffTime = min(
                    CONNECTION_ACQUIRE_MAX_WAIT_SEC,
                    CONNECTION_ACQUIRE_BACKOFF_BASE.toDouble().pow(i).toLong()
                )

                i++
                log.warn("Failed to acquire postgres connection (try #$i), waiting ${backoffTime}s", e)

                try {
                    Thread.sleep(backoffTime * 1000)
                } catch (e: InterruptedException) {
                    isRunning = false
                }
            }
        }

        throw RuntimeException("Postgres connection could not be acquired")
    }

    private fun subscribeToQueues(connection: Connection) {
        connection.createStatement().use { statement ->
            for (queue in PostgresQueue.entries()) {
                statement.execute("LISTEN ${queue.name}")
            }
        }
    }
}
