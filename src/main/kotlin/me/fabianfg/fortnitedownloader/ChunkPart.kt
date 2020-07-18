package me.fabianfg.fortnitedownloader

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import me.fungames.jfortniteparse.util.printHexBinary

class ChunkPart {
    lateinit var chunk : Chunk
        internal set
    lateinit var guid : String
        private set
    var offset = 0
        private set
    var size = 0
        private set

    internal constructor(reader: JsonReader) {
        reader.beginObject()
        while (reader.peek() != JsonToken.END_OBJECT) {
            when(reader.nextName()) {
                "Guid" -> guid = reader.nextString().hexToBytes().printHexBinary()
                "Size" -> size = reader.nextString().hashToInt()
                "Offset" -> offset = reader.nextString().hashToInt()
            }
        }
        reader.endObject()
    }

    internal constructor(chunk: Chunk, offset : Int, size : Int) {
        this.chunk = chunk
        this.guid = chunk.guid
        this.offset = offset
        this.size = size
    }
}