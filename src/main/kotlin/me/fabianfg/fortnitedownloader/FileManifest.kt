package me.fabianfg.fortnitedownloader

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.nio.ByteBuffer
import java.util.*

class FileManifest {

    lateinit var fileName : String
        private set
    lateinit var shaHash : ByteArray
        internal set
    lateinit var chunkParts : List<ChunkPart>
        internal set
    lateinit var installTags : List<String>
        internal set

    val hasHash : Boolean get() = ::shaHash.isInitialized
    val hasInstallTags : Boolean get() = ::shaHash.isInitialized

    internal constructor(fileName : String) {
        this.fileName = fileName
    }

    internal constructor(reader: JsonReader, readFileHashes : Boolean, readInstallTags : Boolean) {
        reader.beginObject()
        while (reader.peek() != JsonToken.END_OBJECT) {
            when(reader.nextName()) {
                "Filename" -> fileName = reader.nextString()
                "FileHash" -> if (readFileHashes) {
                    shaHash = ByteBuffer.allocate(20).hashToBytes(reader.nextString()).array()
                } else {
                    reader.skipValue()
                }
                "FileChunkParts" -> {
                    reader.beginArray()
                    val chunkParts = LinkedList<ChunkPart>()
                    while (reader.peek() != JsonToken.END_ARRAY)
                        chunkParts.add(ChunkPart(reader))
                    this.chunkParts = chunkParts
                    reader.endArray()
                }
                "InstallTags" -> if (readInstallTags) {
                    reader.beginArray()
                    val installTags = LinkedList<String>()
                    while (reader.peek() != JsonToken.END_ARRAY)
                        installTags.add(reader.nextString())
                    reader.endArray()
                    this.installTags = installTags
                } else {
                    reader.skipValue()
                }
            }
        }
        reader.endObject()
    }

    val fileSize : Long
        get() = chunkParts.sumBy { it.size }

    override fun toString() = fileName

    fun getChunkIndex(offset : Long): ChunkSearchResult {
        var offsetI = offset
        for(chunkIndex in chunkParts.indices) {
            if (offsetI < chunkParts[chunkIndex].size) {
                return ChunkSearchResult(chunkIndex, offsetI.toInt())
            }
            offsetI -= chunkParts[chunkIndex].size
        }
        return ChunkSearchResult(-1, -1)
    }
}

data class ChunkSearchResult(val chunkIndex : Int, val chunkOffset : Int)