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

/**
 * Extension function to mount a downloaded manifest
 * @param cachePath The directory to save the cached chunks in. Defaults to ".downloaderChunks" in your current working directory
 * @param chunkPoolCapacity The amount of chunks that should be kept in the in-memory pool. Defaults to 20
 * @param numThreads The amount of threads to use to download concurrently. Defaults to 20
 * @return The mounted build
 * @see MountedBuild
 */
fun Manifest.mount(cachePath: File = File(".downloaderChunks"), chunkPoolCapacity: Int = 20, numThreads: Int = 20) =
    MountedBuild(this, cachePath, chunkPoolCapacity, numThreads)

/**
 * Create a mounted build.
 * This class is thread-safe
 * @param manifest The manifest to use as a basis
 * @param cachePath The directory to save the cached chunks in. Defaults to ".downloaderChunks" in your current working directory
 * @param chunkPoolCapacity The amount of chunks that should be kept in the in-memory pool. Defaults to 20
 * @param numThreads The amount of threads to use to download concurrently. Defaults to 20
 */
class MountedBuild @JvmOverloads constructor(val manifest : Manifest, val cachePath : File = File(".downloaderChunks"), chunkPoolCapacity : Int = 20, numThreads : Int = 20) : CoroutineScope {

    companion object {
        private val logger = KotlinLogging.logger("MountedBuild")
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

    /**
     * Please don't use this scope manually
     * Doing so might slow down any read processes
     */
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

    /**
     * Convenience function for reading from a file by it's name
     *
     * This functions works most efficient with a low amount of large serializations
     *
     * This should not be used for small read operations as it becomes really inefficient
     * For small reads consider using a FManifestPakArchive, because it keeps track of the chunk it's currently working on
     *
     * @param fileName The file to download from
     * @param dest The byte array to write results to
     * @param destOffset The offset to begin writing in the result array
     * @param offset The offset in the file to download from
     * @param length The amount of bytes to read
     * @return whether the operation completed successfully
     *
     * @see fileRead
     */
    fun fileRead(fileName: String, dest: ByteArray, destOffset: Int, offset: Long, length: Int) =
        fileRead(manifest.fileManifestList.first { it.fileName == fileName }, dest, destOffset, offset, length)

    /**
     * Read from a FileManifest into a byte array
     *
     * This functions works most efficient with a low amount of large serializations
     *
     * This should not be used for small read operations as it becomes really inefficient
     * For small reads consider using a FManifestPakArchive, because it keeps track of the chunk it's currently working on
     *
     * @param file The file to download from
     * @param dest The byte array to write results to
     * @param destOffset The offset to begin writing in the result array
     * @param offset The offset in the file to download from
     * @param length The amount of bytes to read
     * @return whether the operation completed successfully
     *
     * @see FManifestPakArchive
     */
    fun fileRead(file: FileManifest, dest: ByteArray, destOffset: Int, offset: Long, length: Int) =
        fileRead(file, dest, getNeededChunks(file, destOffset, offset, length))

    /**
     * This should not be used manually
     *
     * Reads from a file into an byte array by using pre-computed needed chunks
     * Warning: If you attempt to use this manually and provide wrong offsets in the NeededChunk's you might get ArrayIndexOutOfBoundsExceptions
     *
     * @param file The file to download from
     * @param dest The byte array to write results to
     * @param neededChunks The precomputed needed chunks
     * @return whether the operation completed successfully
     */
    internal fun fileRead(file : FileManifest, dest : ByteArray, neededChunks : Iterable<NeededChunk>): Boolean {
        val tasks = neededChunks.map { async { runCatching {
            val chunkBuffer = storage.getChunkPart(it.chunkPart)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk for offset ${it.offset} and size ${it.size} in file ${file.fileName}" }
                return@runCatching false
            } else {
                //logger.info { "Copying ${it.size} bytes to ${it.offset}" }
                chunkBuffer.copyInto(dest, it.offset, it.chunkStartOffset, it.chunkStartOffset + it.size)
                return@runCatching true
            }
        } } }
        val results = runBlocking { tasks.awaitAll() }
        return results.all { result ->
            result.onFailure { logger.warn(it) { "Uncaught exception while downloading chunk" } }
                .getOrElse { false }
        }
    }

    /**
     * Convenience function for preloading a file by it's name
     * Preload = Download the chunk and place it in the cache directory
     *
     * @param fileName The file to preload
     * @param progressUpdate Function to be invoked on progress update or null. First param is the number of chunks already preloaded, Second is the total number of chunks to preload
     * @return whether the operation completed successfully
     *
     * @see preloadChunks
     */
    fun preloadChunks(fileName: String, progressUpdate: ((Int, Int) -> Unit)? = null) =
        preloadChunks(manifest.fileManifestList.first { it.fileName == fileName }, progressUpdate)

    /**
     * Preloads all chunks from a passed file updating the user on the progress
     * Preload = Download the chunk and place it in the cache directory
     *
     * @param file The file to preload
     * @param progressUpdate Function to be invoked on progress update or null. First param is the number of chunks already preloaded, Second is the total number of chunks to preload
     * @return whether the operation completed successfully
     */
    fun preloadChunks(file: FileManifest, progressUpdate: ((Int, Int) -> Unit)? = null): Boolean {
        val readChunks = AtomicInteger(0)
        val tasks = file.chunkParts.mapIndexed { i, it -> async { runCatching {
            val chunkBuffer = storage.getChunkPart(it)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk $i with size ${it.size} in file ${file.fileName}" }
                return@runCatching false
            } else {
                val count = readChunks.incrementAndGet()
                progressUpdate?.invoke(count, file.chunkParts.size)
                return@runCatching true
            }
        } } }
        return runBlocking { tasks.awaitAll() }.all { result ->
            result.onFailure { logger.warn(it) { "Uncaught exception while downloading chunk" } }
                .getOrElse { false }
        }
    }

    /**
     * Convenience function to download an entire file by its name
     *
     * @param file The file to download
     * @param progressUpdate Function to be invoked on progress update or null. First param is the number of bytes already downloaded, Second is the total number of bytes to be read
     * @return whether the operation completed successfully
     *
     * @see downloadEntireFile
     */
    fun downloadEntireFile(fileName: String, output: File, progressUpdate: ((Long, Long) -> Unit)? = null) =
        downloadEntireFile(manifest.fileManifestList.first { it.fileName == fileName }, output, progressUpdate)
    /**
     * Downloads an entire FileManifest to a passed output file updating the user on the progress
     *
     * @param file The file to download
     * @param progressUpdate Function to be invoked on progress update or null. First param is the number of bytes already downloaded, Second is the total number of bytes to be read
     * @return whether the operation completed successfully
     */
    @Suppress("BlockingMethodInNonBlockingContext")
    fun downloadEntireFile(file: FileManifest, output: File, progressUpdate: ((Long, Long) -> Unit)? = null): Boolean {
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
        val tasks = file.chunkParts.mapIndexed { i, it -> async { runCatching {
            val chunkBuffer = storage.getChunkPart(it)
            if (chunkBuffer == null) {
                logger.error { "Failed to download chunk $i with size ${it.size} in file ${file.fileName}" }
                return@runCatching false
            } else {
                fileWriteMutex.withLock {
                    raFile.seek(offsets[i])
                    raFile.write(chunkBuffer)
                }
                val newReadBytes = readBytes.addAndGet(chunkBuffer.size.toLong())
                progressUpdate?.invoke(newReadBytes, totalSize)
                return@runCatching true
            }
        } } }
        val res = runBlocking { tasks.awaitAll() }.all { result ->
            result.onFailure { logger.warn(it) { "Uncaught exception while downloading chunk" } }
                .getOrElse { false }
        }
        raFile.close()
        return res
    }
}