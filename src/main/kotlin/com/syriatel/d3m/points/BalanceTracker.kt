package com.syriatel.d3m.points

import com.syriatel.d3m.utils.JsonSerde
import com.syriatel.d3m.utils.StoresProvider
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Aggregator
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Initializer


val balanceTracker: (StreamsBuilder, StoresProvider) -> Unit = { builder, supplier ->
    builder.stream<String, PointTransaction>("transactions", Consumed.with(Serdes.String(), JsonSerde.of()))
            .groupByKey().aggregate(
                    Initializer {
                        Balance(0L)
                    },
                    Aggregator { _: String, t: PointTransaction, b: Balance ->
                        t.execute(b)
                    },
                    supplier.keyValueStore("customer-balance", Serdes.String(), JsonSerde.of())
            )
}