package me.fabianfg.fortnitedownloader

import me.fungames.jfortniteparse.ue4.reader.FArchive
import mu.KotlinLogging
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.internal.closeQuietly
import java.io.File
import java.io.InputStream
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.InflaterInputStream
import kotlin.concurrent.withLock

private enum class ChunkStatus {
    Unavailable, // Readable from download
    Grabbing,    // Downloading
    Available,   // Readable from local copy
    Reading,     // Reading from local copy
    Readable     // Readable from memory
}

private class ChunkPoolData(
    var buffer : ByteArray? = null,
    val status : AtomicReference<ChunkStatus>
) {
    val mutex = ReentrantLock()
    val cv = mutex.newCondition()
}

class Storage internal constructor(val chunkPoolCapacity : Int, val cacheLocation : String, val cloudDir : String) {

    companion object {
        val logger = KotlinLogging.logger("MountedBuild")
    }

    init {
        File(cacheLocation).mkdirs()
    }

    private val client = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .build()
    private val chunkPool = HashMap<String, ChunkPoolData>(chunkPoolCapacity)
    private val chunkPoolMutex = ReentrantLock()

    fun getChunkPart(chunkPart: ChunkPart): ByteArray? {
        val chunk = getChunk(chunkPart.chunk)
        return if (chunk == null || chunkPart.offset == 0 && chunkPart.size == chunkPart.chunk.windowSize)
            chunk
        else {
            val partBuffer = ByteArray(chunkPart.size)
            chunk.copyInto(partBuffer, 0, chunkPart.offset, chunkPart.offset + chunkPart.size)
            partBuffer
        }
    }

    fun getChunk(chunk: Chunk): ByteArray? {
        val data = getPoolData(chunk)

        fun doDownload() {
            // download
            data.status.set(ChunkStatus.Grabbing)
            val chunkData = downloadChunk(chunk)
            if (chunkData != null) {
                // I guess that really can't happen
                //if (chunkData.size != chunk.windowSize)
                //    logger.error { "Failed to download chunk ${chunk.url()} correctly" }
                data.buffer = chunkData
                data.status.set(ChunkStatus.Readable)
            } else {
                data.buffer = null
                data.status.set(ChunkStatus.Unavailable)
            }
            data.mutex.withLock {
                data.cv.signalAll()
            }
        }

        when(data.status.get()) {
            ChunkStatus.Unavailable -> {
                doDownload()
            }
            ChunkStatus.Available -> {
                // read from file
                data.status.set(ChunkStatus.Reading)

                val chunkData = runCatching { File(cacheLocation + chunk.fileName()).readBytes() }.getOrNull()
                if (chunkData == null || chunkData.size != chunk.windowSize) {
                    // That can / will happen if the chunk is very new on the file system
                    // It can also happen if the chunk file just got deleted
                    if (chunkData == null)
                        logger.warn { "Failed to read chunk ${chunk.fileName()} from file, didn't receive anything. Why tho" }
                    else
                        logger.warn { "Failed to read chunk ${chunk.fileName()} from file. Expected ${chunk.windowSize} bytes, got ${chunkData.size} bytes. Why tho" }
                    doDownload()
                    if (data.status.get() == ChunkStatus.Readable)
                        logger.info { "Re-download of previously failed chunk ${chunk.fileName()} succeeded" }
                    else
                        logger.info { "Re-download of previously failed chunk ${chunk.fileName()} also failed" }
                } else {
                    data.buffer = chunkData
                    data.status.set(ChunkStatus.Readable)
                }
                data.mutex.withLock {
                    data.cv.signalAll()
                }
            }
            ChunkStatus.Grabbing, ChunkStatus.Reading -> {
                // downloading from server / reading from file, wait until mutex releases
                data.mutex.withLock {
                    data.cv.await()
                }
            }
            ChunkStatus.Readable -> {
                // available in memory pool
            }
            else -> {
                // how
            }
        }
        return data.buffer
    }

    private fun getPoolData(chunk: Chunk) = chunkPoolMutex.withLock {
        for (storedChunk in chunkPool) {
            if (storedChunk.key == chunk.guid)
                return@withLock storedChunk.value
        }

        while (chunkPool.size >= chunkPoolCapacity) {
            chunkPool.remove(chunkPool.keys.first())
        }

        val data = ChunkPoolData(
            null,
            AtomicReference(getUnpooledChunkStatus(chunk))
        )
        chunkPool[chunk.guid] = data
        data
    }

    private fun getUnpooledChunkStatus(chunk: Chunk) =
        if (isChunkDownloaded(chunk)) ChunkStatus.Available else ChunkStatus.Unavailable

    fun isChunkDownloaded(chunk: Chunk) =
        File(cacheLocation + chunk.fileName()).exists()

    @Suppress("EXPERIMENTAL_API_USAGE")
    fun downloadChunk(chunk: Chunk) = runCatching {
        val response = client.newCall(Request.Builder()
            .url(cloudDir + chunk.url())
            .addHeader("Accept-Encoding", "br, gzip, deflate")
            .build()).execute()
        val body = response.body
        if (response.isSuccessful && body != null) {
            val data = body.use {
                val chunkData = body.byteStream()
                val reader = FStreamArchive(chunkData)
                reader.skip(4)
                val version = reader.readInt32()
                val headerSize = reader.readInt32()
                reader.skip(28)
                val storedAs = reader.readUInt8().toInt()
                val decompressedSize = if (version >= 3) {
                    reader.skip(21)
                    reader.readInt32()
                }
                else
                    1024 * 1024
                reader.skip((headerSize - reader.pos()).toLong())

                if (storedAs and 0x02 != 0) {
                    throw UnsupportedOperationException("Encrypted chunks are not supported (yet)")
                }

                if (storedAs and 0x01 != 0) {
                    val out = ModByteArrayOutputStream(decompressedSize)
                    val decompress = InflaterInputStream(chunkData)
                    decompress.copyTo(out)
                    out.size()
                    out.toByteArray()
                } else {
                    val out = ModByteArrayOutputStream(chunk.windowSize)
                    chunkData.copyTo(out)
                    out.toByteArray()
                }
            }
            File(cacheLocation + chunk.fileName()).writeBytes(data)
            data
        } else {
            logger.warn { "Received response code ${response.code} for chunk ${response.request.url}" }
            response.closeQuietly()
            return@runCatching null
        }
    }.onFailure { logger.warn(it) { "Exception while attempting to download chunk ${cloudDir + chunk.url()}" } }.getOrNull()
}

@Suppress("EXPERIMENTAL_OVERRIDE", "EXPERIMENTAL_API_USAGE")
private class FStreamArchive(val input : InputStream) : FArchive() {
    override var littleEndian = true

    private var consumedBytes = 0

    override fun clone() = FStreamArchive(input)

    override fun pos() = consumedBytes

    override fun printError() = "FStreamArchive Info: pos $consumedBytes"

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        val res = input.read(b, off, len)
        consumedBytes += res
        return res
    }

    override fun read(): Int {
        val res = input.read()
        if (res >= 0)
            consumedBytes++
        return res
    }

    override fun seek(pos: Int) {
       throw UnsupportedOperationException("Seeking not supported")
    }

    override fun size(): Int {
        throw UnsupportedOperationException("Size not supported")
    }

    override fun skip(n: Long): Long {
        val res = input.skip(n)
        consumedBytes += res.toInt()
        return res
    }

}