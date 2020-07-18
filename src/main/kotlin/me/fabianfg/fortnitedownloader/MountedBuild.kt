package me.fabianfg.fortnitedownloader

import kotlinx.coroutines.*
import mu.KotlinLogging
import java.io.File
import java.io.RandomAccessFile
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

fun Manifest.mount(cachePath: File = File(".downloaderChunks"), chunkPoolCapacity: Int = 20, numThreads: Int = 20) =
    MountedBuild(this, cachePath, chunkPoolCapacity, numThreads)

class MountedBuild(val manifest : Manifest, val cachePath : File = File(".downloaderChunks"), chunkPoolCapacity : Int = 20, numThreads : Int = 20) : CoroutineScope {

    companion object {
        val logger = KotlinLogging.logger("MountedBuild")
    }

    private val job = Job()
    private val ex = Executors.newFixedThreadPool(numThreads, object : ThreadFactory {
        private var count = 0

        override fun newThread(r: Runnable): Thread {
            val thread = Thread(r, "MountedBuild-Thread-$count")
            thread.isDaemon = true
            count++
            return thread
        }
    })
    override val coroutineContext = job + ex.asCoroutineDispatcher()
    private val storage = Storage(
        chunkPoolCapacity,
        cachePath.absolutePath,
        manifest.cloudDir
    )

    internal data class NeededChunk(val chunkPart: ChunkPart, val chunkStartOffset : Int,
                                    val offset : Int, val size : Int)

    private fun getNeededChunks(file: FileManifest, destOffset: Int, offset: Long, length: Int): Iterable<NeededChunk> {
        var (chunkStartIndex, chunkStartOffset) = file.getChunkIndex(offset)
        require(chunkStartIndex >= 0) { "Invalid offset" }

        var bytesRead = 0
        val neededChunks = mutableListOf<NeededChunk>()
        for (chunkPartIndex in chunkStartIndex until file.chunkParts.size) {
            val chunkPart = file.chunkParts[chunkPartIndex]
            if ((length - bytesRead) > chunkPart.size - chunkStartOffset) {
                // copy the entire buffer over
                neededChunks.add(
                    NeededChunk(
                        chunkPart,
                        chunkStartOffset,
                        destOffset + bytesRead,
                        (chunkPart.size - chunkStartOffset)
                    )
                )
                bytesRead += (chunkPart.size - chunkStartOffset)
            } else {
                // copy what it needs to fill up the rest
                neededChunks.add(
                    NeededChunk(
                        chunkPart,
                        chunkStartOffset,
                        destOffset + bytesRead,
                        length - bytesRead
                    )
                )
                bytesRead += length - bytesRead
                break
            }
            chunkStartOffset = 0
        }
        return neededChunks
    }

    fun fileRead(fileName: String, dest: ByteArray, destOffset: Int, offset: Long, length: Int) =
        fileRead(manifest.fileManifestList.first { it.fileName == fileName }, dest, destOffset, offset, length)
    fun fileRead(file: FileManifest, dest: ByteArray, destOffset: Int, offset: Long, length: Int) =
        fileRead(file, dest, getNeededChunks(file, destOffset, offset, length))

    internal fun fileRead(file : FileManifest, dest : ByteArray, neededChunks : Iterable<NeededChunk>): Boolean {
        val tasks = neededChunks.map { async {
            val chunkBuffer = storage.getChunkPart(it.chunkPart)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk for offset ${it.offset} and size ${it.size} in file ${file.fileName}" }
                return@async false
            } else {
                //logger.info { "Copying ${it.size} bytes to ${it.offset}" }
                chunkBuffer.copyInto(dest, it.offset, it.chunkStartOffset, it.chunkStartOffset + it.size)
                return@async true
            }
        } }
        val results = runBlocking { tasks.awaitAll() }
        return results.all { it }
    }

    fun preloadChunks(fileName: String, progressUpdate: ((Int, Int) -> Unit)? = null) =
        preloadChunks(manifest.fileManifestList.first { it.fileName == fileName }, progressUpdate)
    fun preloadChunks(file: FileManifest, progressUpdate: ((Int, Int) -> Unit)? = null): Boolean {
        val readChunks = AtomicInteger(0)
        val tasks = file.chunkParts.mapIndexed { i, it -> async {
            val chunkBuffer = storage.getChunkPart(it)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk $i with size ${it.size} in file ${file.fileName}" }
                return@async false
            } else {
                val count = readChunks.incrementAndGet()
                progressUpdate?.invoke(count, file.chunkParts.size)
                return@async true
            }
        } }
        return runBlocking { tasks.awaitAll() }.all { it }
    }

    fun downloadEntireFile(fileName: String, output: File, progressUpdate: (Long, Long) -> Unit) =
        downloadEntireFile(manifest.fileManifestList.first { it.fileName == fileName }, output, progressUpdate)
    @Suppress("BlockingMethodInNonBlockingContext")
    fun downloadEntireFile(file: FileManifest, output: File, progressUpdate: (Long, Long) -> Unit): Boolean {
        val offsets = LongArray(file.chunkParts.size)
        var totalSize = 0L
        file.chunkParts.forEachIndexed { i, chunkPart ->
            offsets[i] = totalSize
            totalSize += chunkPart.size
        }
        //Create the file and align to correct size
        val raFile = RandomAccessFile(output, "rw")
        raFile.setLength(totalSize)
        val fileWriteMutex = ReentrantLock()
        val readBytes = AtomicLong(0L)
        val tasks = file.chunkParts.mapIndexed { i, it -> async {
            val chunkBuffer = storage.getChunkPart(it)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk $i with size ${it.size} in file ${file.fileName}" }
                return@async false
            } else {
                fileWriteMutex.withLock {
                    raFile.seek(offsets[i])
                    raFile.write(chunkBuffer)
                }
                val newReadBytes = readBytes.addAndGet(chunkBuffer.size.toLong())
                progressUpdate.invoke(newReadBytes, totalSize)
                return@async true
            }
        } }
        val res = runBlocking { tasks.awaitAll() }.all { it }
        raFile.close()
        return res
    }
}