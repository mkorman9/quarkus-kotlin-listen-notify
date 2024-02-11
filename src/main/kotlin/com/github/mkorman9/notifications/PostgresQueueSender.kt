package com.github.mkorman9.notifications

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.enterprise.context.ApplicationScoped
import org.postgresql.jdbc.PgConnection
import javax.sql.DataSource

@ApplicationScoped
class PostgresQueueSender(
    private val dataSource: DataSource,
    private val objectMapper: ObjectMapper
) {
    fun send(queue: PostgresQueue, payload: Any) {
        val payloadSerialized = objectMapper.writeValueAsString(payload)
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                val pgConnection = connection.unwrap(PgConnection::class.java)
                val escapedPayload = pgConnection.escapeString(payloadSerialized)
                statement.execute("NOTIFY ${queue.name}, '$escapedPayload'")
            }
        }
    }
}
