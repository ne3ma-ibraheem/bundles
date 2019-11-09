package com.syriatel.d3m.points

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.lang.Exception
import java.time.ZonedDateTime

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes(
        JsonSubTypes.Type(value = PointTransaction.ResetPoints::class, name = "reset"),
        JsonSubTypes.Type(value = PointTransaction.MutateBalance::class, name = "transaction")
)
sealed class PointTransaction {


    data class ResetPoints(val timestamp: ZonedDateTime) : PointTransaction() {
        override fun execute(balance: Balance): Balance = Balance(0L)
    }

    data class MutateBalance(
            val timestamp: ZonedDateTime,
            val amount: Long
    ) : PointTransaction() {
        override fun execute(balance: Balance): Balance =
                if (balance.amount + amount >= 0)
                    balance.copy(amount = balance.amount + amount)
                else throw NoBalanceException()
    }

    abstract fun execute(balance: Balance): Balance
}

class NoBalanceException() : Exception()

