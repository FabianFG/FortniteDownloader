package me.fabianfg.fortnitedownloader

import java.nio.ByteBuffer
import java.nio.ByteOrder

internal fun ByteArray.swapOrder(index : Int, size : Int) : ByteArray {
    require(size % 2 == 0) { "size must be even" }

    var backwardsIndex = index + size - 1
    for (i in index until index + size / 2) {
        this[i] = this[backwardsIndex].also { this[backwardsIndex] = this[i] }
        backwardsIndex--
    }
    return this
}

private fun getByteValue(char : Char) = when (char) {
    in '0'..'9' -> (char - '0')
    in 'A'..'F' -> ((char - 'A') + 10)
    else -> ((char - 'a') + 10)
}

internal fun String.hexToBytes() : ByteArray {
    val output = ByteArray(this.length / 2)
    for (i in this.indices step 2) {
        output[i / 2] = (getByteValue(this[i]) shl 4).toByte()
        output[i / 2] = (output[i / 2] + getByteValue(this[i + 1])).toByte()
    }
    return output
}

internal fun ByteBuffer.hashToBytes(data : String) : ByteBuffer {
    val size = data.length / 3
    for (i in 0 until size) {
        this.put(i, data.substring(i * 3, i * 3 + 3).toInt().toByte())
    }
    return this
}

internal fun String.hashToInt() =
    ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).hashToBytes(this).int
internal fun String.hashToLong() =
    ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).hashToBytes(this).long
internal fun hashToHexString(data : String) {
    var hexString = ""
    for (i in data.indices step 3) {
        val int = data.substring(i, i + 3).toInt()
        hexString = "%02X".format(int) + hexString
    }
}

inline fun <reified T> Iterable<T>.sumBy(selector: (T) -> Int): Long {
    var sum: Long = 0
    for (element in this) {
        sum += selector(element)
    }
    return sum
}