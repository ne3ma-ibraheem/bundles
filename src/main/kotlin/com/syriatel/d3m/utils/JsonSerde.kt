package com.syriatel.d3m.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class JsonSerializer<T>(val mapper: ObjectMapper) : Serializer<T> {
    override fun serialize(key: String?, value: T): ByteArray =
            mapper.writeValueAsBytes(value)
}

class JsonDeserializer<T>(
        val clazz: Class<T>,
        val mapper: ObjectMapper
) : Deserializer<T> {
    override fun deserialize(key: String?, bytes: ByteArray?): T? =
            if (bytes !== null) mapper.readValue(bytes, clazz)
            else
                null

}

class JsonSerde<T>(
        val clazz: Class<T>,
        val mapper: ObjectMapper
) : Serde<T> {
    override fun serializer(): Serializer<T> = JsonSerializer(mapper)
    override fun deserializer(): Deserializer<T> = JsonDeserializer(clazz, mapper)

    companion object {
        val mapper = initObjectMapper()
        inline fun <reified T> of() = JsonSerde(T::class.java, mapper)
    }
}


fun initObjectMapper(): ObjectMapper = ObjectMapper().findAndRegisterModules()
        .registerKotlinModule().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);