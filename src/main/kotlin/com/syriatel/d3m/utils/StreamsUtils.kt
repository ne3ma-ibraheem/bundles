package com.syriatel.d3m.utils

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.*
import java.time.Duration
import java.util.*

fun topology(fn: StreamsBuilder.() -> Unit): Topology = StreamsBuilder().apply(fn).build()

fun properties(fn: Properties.() -> Unit): Properties = Properties().apply(fn)

interface StoresProvider {
    val keyValueStore: (String) -> KeyValueBytesStoreSupplier
    val windowStore: (String, Duration, Duration, Boolean) -> WindowBytesStoreSupplier
    val sessionStore: (String, Duration) -> SessionBytesStoreSupplier

    fun <K, V> keyValueStore(name: String, keySerde: Serde<K>, valueSerde: Serde<V>): Materialized<K, V, KeyValueStore<Bytes, ByteArray>> =
            Materialized.`as`<K, V>(keyValueStore(name)).withKeySerde(keySerde).withValueSerde(valueSerde)
}

object InMemoryStores : StoresProvider {
    override val keyValueStore = Stores::inMemoryKeyValueStore
    override val windowStore = Stores::inMemoryWindowStore
    override val sessionStore = Stores::inMemorySessionStore
}

object PersistentStores : StoresProvider {
    override val keyValueStore = Stores::persistentKeyValueStore
    override val windowStore = Stores::persistentTimestampedWindowStore
    override val sessionStore = { name: String, duration: Duration ->
        Stores.persistentSessionStore(name, duration)
    }
}
