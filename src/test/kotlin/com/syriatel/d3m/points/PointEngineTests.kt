package com.syriatel.d3m.points

import com.syriatel.d3m.utils.InMemoryStores
import com.syriatel.d3m.utils.JsonSerde
import com.syriatel.d3m.utils.properties
import com.syriatel.d3m.utils.topology

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.*


class PointEngineTests {
    val pointsFactory = ConsumerRecordFactory<String/*GSM*/, PointTransaction>(
            StringSerializer(),
            JsonSerde.of<PointTransaction>().serializer()
    )
    val driver = initTestDriver({
        this[StreamsConfig.APPLICATION_ID_CONFIG] = "test-application"
        this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
    }) {
        balanceTracker(this, InMemoryStores)
    }

    @Test
    fun `should add points to balance`() {
        val balance = driver.getKeyValueStore<String, Balance>("customer-balance")
        balance.put("0988957030", Balance(0))
        driver.pipeInput(
                pointsFactory.create("transactions",
                        "0988957030",
                        PointTransaction.MutateBalance(ZonedDateTime.now(), 10)
                )
        )
        assertThat(balance["0988957030"], equalTo(Balance(10)))
    }

    @Test
    fun `should deduce points to balance`() {
        val balance = driver.getKeyValueStore<String, Balance>("customer-balance")
        balance.put("0988957030", Balance(30))
        driver.pipeInput(
                pointsFactory.create("transactions",
                        "0988957030",
                        PointTransaction.MutateBalance(ZonedDateTime.now(), -10)
                )
        )
        assertThat(balance["0988957030"], equalTo(Balance(20)))
    }

    @Test
    fun `should throw no balance exception when no balance`() {
        val balance = driver.getKeyValueStore<String, Balance>("customer-balance")
        balance.put("0988957030", Balance(1))
        Assertions.assertThrows(NoBalanceException::class.java) {
            driver.pipeInput(
                    pointsFactory.create("transactions",
                            "0988957030",
                            PointTransaction.MutateBalance(ZonedDateTime.now(), -10)
                    )
            )
        }
        assertThat(balance["0988957030"], equalTo(Balance(1)))
    }

    @Test
    fun `should reset balance`() {
        val balance = driver.getKeyValueStore<String, Balance>("customer-balance")
        balance.put("0988957030", Balance(30))
        driver.pipeInput(
                pointsFactory.create("transactions",
                        "0988957030",
                        PointTransaction.ResetPoints(ZonedDateTime.now())
                )
        )
        assertThat(balance["0988957030"], equalTo(Balance(0)))
    }
}


fun initTestDriver(propertiesProvider: Properties.() -> Unit = {}, topologyProvider: StreamsBuilder.() -> Unit) =
        TopologyTestDriver(topology(topologyProvider), properties(propertiesProvider))