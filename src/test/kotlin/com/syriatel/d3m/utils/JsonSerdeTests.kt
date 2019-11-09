package com.syriatel.d3m.utils

import com.jayway.jsonpath.matchers.JsonPathMatchers.*
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.nio.charset.Charset

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

@DisplayName("Json Serde Tests")
class JsonSerdeTests {

    data class TestDataClass(
            val int: Int,
            val time: OffsetDateTime
    )

    val serializer = JsonSerializer<TestDataClass>(initObjectMapper())
    val deserializer = JsonDeserializer(TestDataClass::class.java, initObjectMapper())

    @Test
    fun `should serialize pojo to json`() {
        val time = OffsetDateTime.now()
        val result = serializer.serialize(
                null, TestDataClass(10, time)
        )

        assertThat(
                result.toString(Charset.defaultCharset()),
                isJson(
                        allOf(
                                hasJsonPath("$.int", equalTo(10)),
                                hasJsonPath("$.time", equalTo(
                                        time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                                )
                        )
                )

        )

    }

    @Test
    fun `should deserialize json to pojo`() {
        val json = """{
                "int": 10,
                "time": "2019-10-10T08:10:10.000Z"
            }
        """.trimIndent()
        val result = deserializer.deserialize(null, json.toByteArray())
        val expected = TestDataClass(
                10,
                OffsetDateTime.parse("2019-10-10T08:10:10.000Z")
        )
        assertThat(
                result,
                equalTo(expected)
        )

    }
}