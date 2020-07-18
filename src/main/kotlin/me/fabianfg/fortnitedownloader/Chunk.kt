package me.fabianfg.fortnitedownloader

import me.fungames.jfortniteparse.util.printHexBinary

@Suppress("EXPERIMENTAL_API_USAGE")
class Chunk {
    lateinit var guid : String
        internal set
    var hash : Long = 0
        internal set
    lateinit var shaHash : ByteArray
        internal set
    var group : Int = 0
        internal set
    var windowSize = 1048576 // https://github.com/EpicGames/UnrealEngine/blob/f8f4b403eb682ffc055613c7caf9d2ba5df7f319/Engine/Source/Runtime/Online/BuildPatchServices/Private/Data/ChunkData.cpp#L246 (default constructor)
        internal set

    internal constructor(guid : ByteArray) {
        this.guid = guid.printHexBinary()
    }

    internal constructor()

    //val guid = guid/*.swapOrder(0, 8).swapOrder(8, 8)*/.printHexBinary()

    fun fileName() = "/${group}_$guid"
    fun url() = "%02d/%016X_%s.chunk".format(group, hash, guid)
}