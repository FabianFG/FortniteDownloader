package me.fabianfg.fortnitedownloader

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import me.fungames.jfortniteparse.compression.Compression
import me.fungames.jfortniteparse.ue4.pak.CompressionMethod
import me.fungames.jfortniteparse.ue4.reader.FByteArchive
import me.fungames.jfortniteparse.util.printHexBinary
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap


class Manifest {
    lateinit var featureLevel : EFeatureLevel
        private set
    var isFileData = false
        private set
    var appID = 0
        private set
    lateinit var appName : String
        private set
    lateinit var buildVersion : String
        private set
    lateinit var launchExe : String
        private set
    lateinit var launchCommand : String
        private set
    lateinit var cloudDir : String
        private set

    val chunkManifestList : List<Chunk>
    val fileManifestList : List<FileManifest>

    private constructor(json: String, cloudDir: String, readOptionalValues : Boolean) {
        val reader = JsonReader(json.reader())
        reader.beginObject()
        val chunkManifestList = LinkedList<Chunk>()
        val fileManifestList = LinkedList<FileManifest>()
        val chunkManifestLookup = HashMap<String, Chunk>()
        loop@ while (reader.peek() != JsonToken.END_OBJECT) {
            if (reader.peek() != JsonToken.NAME) {
                reader.skipValue()
                continue@loop
            }

            when(reader.nextName()) {
                "ManifestFileVersion" -> {
                    featureLevel = EFeatureLevel.safeValue(
                        reader.nextString().hashToInt()
                    )
                    val chunksDir = when {
                        featureLevel < EFeatureLevel.DataFileRenames -> "/Chunks/"
                        featureLevel < EFeatureLevel.ChunkCompressionSupport -> "/ChunksV2/"
                        featureLevel < EFeatureLevel.VariableSizeChunksWithoutWindowSizeChunkInfo -> "/ChunksV3/"
                        else -> "/ChunksV4/"
                    }
                    this.cloudDir = cloudDir + chunksDir
                }
                "bIsFileData" -> isFileData = reader.nextBoolean()
                "AppID" -> appID = reader.nextString().hashToInt()
                "AppNameString" -> appName = reader.nextString()
                "BuildVersionString" -> buildVersion = reader.nextString()
                "LaunchExeString" -> launchExe = reader.nextString()
                "LaunchCommand" -> launchCommand = reader.nextString()
                "FileManifestList" -> {
                    reader.beginArray()
                    while(reader.peek() != JsonToken.END_ARRAY) {
                        fileManifestList.add(
                            FileManifest(
                                reader,
                                readOptionalValues
                            )
                        )
                    }
                    reader.endArray()
                }
                "ChunkHashList" -> {
                    reader.beginObject()
                    if (chunkManifestList.isEmpty()) {
                        while (reader.peek() != JsonToken.END_OBJECT) {
                            val chunk = Chunk()
                            val guid = reader.nextName().hexToBytes().printHexBinary()
                            chunk.guid = guid
                            chunkManifestLookup[guid] = chunk
                            chunk.hash = reader.nextString().hashToLong()
                            chunkManifestList.add(chunk)
                        }
                    } else {
                        chunkManifestList.forEach { chunk ->
                            reader.skipValue()
                            chunk.hash = reader.nextString().hashToLong()
                        }
                        while (reader.peek() != JsonToken.END_OBJECT)
                            reader.skipValue()
                    }
                    reader.endObject()
                }
                "ChunkShaList" -> if (readOptionalValues) {
                    reader.beginObject()
                    if (chunkManifestList.isEmpty()) {
                        while (reader.peek() != JsonToken.END_OBJECT) {
                            val chunk = Chunk()
                            chunk.guid = reader.nextName().hexToBytes().printHexBinary()
                            chunk.shaHash = reader.nextString().hexToBytes()
                            chunkManifestList.add(chunk)
                        }
                    } else {
                        chunkManifestList.forEach { chunk ->
                            reader.skipValue()
                            chunk.shaHash = reader.nextString().hexToBytes()
                        }
                        while (reader.peek() != JsonToken.END_OBJECT)
                            reader.skipValue()
                    }
                    reader.endObject()
                } else {
                    reader.skipValue()
                }
                "DataGroupList" -> {
                    reader.beginObject()
                    if (chunkManifestList.isEmpty()) {
                        while (reader.peek() != JsonToken.END_OBJECT) {
                            val chunk = Chunk()
                            chunk.guid = reader.nextName().hexToBytes().printHexBinary()
                            chunk.group = reader.nextInt()
                            chunkManifestList.add(chunk)
                        }
                    } else {
                        chunkManifestList.forEach { chunk ->
                            reader.skipValue()
                            chunk.group = reader.nextInt()
                        }
                        while (reader.peek() != JsonToken.END_OBJECT)
                            reader.skipValue()
                    }
                    reader.endObject()
                }
                //ChunkFileSizeList, CustomFields: not needed
                else -> reader.skipValue()
            }
        }
        this.chunkManifestList = chunkManifestList
        this.fileManifestList = fileManifestList
        //Map ChunkParts with their chunks
        fileManifestList.forEach { file ->
            file.chunkParts.forEach {
                it.chunk = chunkManifestLookup[it.guid]!!
            }
        }
        reader.endObject()
        reader.close()
    }

    @Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")
    private constructor(buffer : ByteBuffer, manifestUrl: String, readOptionalValues : Boolean) {
        val reader = FByteArchive(buffer)
        val headerSize = reader.readInt32()
        val dataSizeUncompressed = reader.readInt32()
        val dataSizeCompressed = reader.readInt32()
        reader.skip(20) // ShaHash
        val storedAs = reader.readUInt8().toInt()
        reader.skip(4) // Version

        reader.seek(headerSize) // make sure it's past the header

        val data = ByteArray(dataSizeUncompressed)
        if (storedAs and 0x01 != 0) {
            // compressed
            val compressed = reader.read(dataSizeCompressed)
            Compression.decompress(compressed, data, CompressionMethod.Zlib)
        } else {
            reader.read(data)
        }

        if (storedAs and 0x02 != 0) {
            // encrypted, never used yet
            throw UnsupportedOperationException("Encrypted Manifests are not supported yet")
        }

        val manifestData = FByteArchive(data)

        var curPos = manifestData.pos()
        // FManifestMeta
        var dataSize = manifestData.readInt32()
        var dataVersion = manifestData.readUInt8()

        featureLevel =
            EFeatureLevel.safeValue(manifestData.readInt32())
        val chunksDir = when {
            featureLevel < EFeatureLevel.DataFileRenames -> "/Chunks/"
            featureLevel < EFeatureLevel.ChunkCompressionSupport -> "/ChunksV2/"
            featureLevel < EFeatureLevel.VariableSizeChunksWithoutWindowSizeChunkInfo -> "/ChunksV3/"
            else -> "/ChunksV4/"
        }
        cloudDir = manifestUrl + chunksDir
        isFileData = manifestData.readFlag()
        appID = manifestData.readInt32()
        appName = manifestData.readString()
        buildVersion = manifestData.readString()
        launchExe = manifestData.readString()
        launchCommand = manifestData.readString()
        /*val prereqIds = */manifestData.readTArray { it.readString() }
        /*val prereqName = */manifestData.readString()
        /*val prereqPath = */manifestData.readString()
        /*val prereqArgs = */manifestData.readString()

        if (dataVersion >= 1u) {
            /*val buildId = */manifestData.readString()
        }
        manifestData.seek(curPos + dataSize)

        curPos = manifestData.pos()

        // FChunkDataList
        dataSize = manifestData.readInt32()
        dataVersion = manifestData.readUInt8()

        var count = manifestData.readInt32()
        val chunkManifestLookup = HashMap<String, Int>(count)
        chunkManifestList = ArrayList(count)

        if (dataVersion >= 0u) { // Should be useless
            for (i in 0 until count) {
                chunkManifestList.add(
                    Chunk(
                        manifestData.read(16)
                            .swapOrder(0, 4)
                            .swapOrder(4, 4)
                            .swapOrder(8, 4)
                            .swapOrder(12, 4)
                    )
                )
                chunkManifestLookup[chunkManifestList[i].guid] = i
            }
            for (c in chunkManifestList) { c.hash = manifestData.readInt64() }
            if (readOptionalValues)
                for (c in chunkManifestList) { c.shaHash = manifestData.read(20) }
            else
                manifestData.skip(count * 20L)
            for (c in chunkManifestList) { c.group = manifestData.readUInt8().toInt() }
            for (c in chunkManifestList) { c.windowSize = manifestData.readInt32() }
            manifestData.skip(count * 8L) // FileSize
        }

        manifestData.seek(curPos + dataSize)

        curPos = manifestData.pos()

        // FFileManifestList
        dataSize = manifestData.readInt32()
        dataVersion = manifestData.readUInt8()

        count = manifestData.readInt32()
        fileManifestList = ArrayList(count)

        if (dataVersion >= 0u) { // Again should be useless
            for (i in 0 until count) { fileManifestList.add(
                FileManifest(
                    manifestData.readString()
                )
            ) }
            for (i in 0 until count) { manifestData.readString() } // SymlinkTarget
            if (readOptionalValues)
                for (i in 0 until count) { fileManifestList[i].shaHash = manifestData.read(20) }
            else
                manifestData.skip(count * 20L)
            manifestData.skip(count.toLong()) // FileMetaFlags
            if (readOptionalValues)
                for (i in 0 until count) { fileManifestList[i].installTags = manifestData.readTArray { it.readString() }.toList() }
            else
                for (i in 0 until count) { manifestData.readTArray { it.readString() } } // InstallTags
            for (f in fileManifestList) {
                val chunkCount = manifestData.readInt32()
                val list = ArrayList<ChunkPart>(chunkCount)
                f.chunkParts = list
                for (i in 0 until chunkCount) {
                    manifestData.skip(4)
                    val guid = manifestData.read(16)
                        .swapOrder(0, 4)
                        .swapOrder(4, 4)
                        .swapOrder(8, 4)
                        .swapOrder(12, 4)
                        .printHexBinary()
                    val ret = ChunkPart(
                        chunkManifestList[chunkManifestLookup[guid]!!],
                        manifestData.readInt32(),
                        manifestData.readInt32()
                    )
                    list.add(ret)
                }
            }
        }

        manifestData.seek(curPos + dataSize)

        curPos = manifestData.pos()

        // FCustomFields
        dataSize = manifestData.readInt32()
        dataVersion = manifestData.readUInt8()

        count = manifestData.readInt32()

        if (dataVersion >= 0u) { // Again again should be useless
            for (i in 0 until count) { manifestData.readString() }
            for (i in 0 until count) { manifestData.readString() }
        }

        manifestData.seek(curPos + dataSize)
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        @Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")
        fun parse(data: ByteArray, url : String, readOptionalValues: Boolean = false) : Manifest {
            var cloudDir = url.replace('\\', '/')
            if (cloudDir.substringAfterLast('/').substringBeforeLast('?').endsWith(".manifest"))
                cloudDir = cloudDir.substringBeforeLast('/')
            val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
            return if (buffer.int.toUInt() == 0x44BEC00Cu)
                Manifest(buffer, cloudDir, readOptionalValues)
            else
                Manifest(
                    String(data),
                    cloudDir,
                    readOptionalValues
                )
        }
    }
}