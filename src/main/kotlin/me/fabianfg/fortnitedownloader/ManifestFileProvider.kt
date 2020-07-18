package me.fabianfg.fortnitedownloader

import kotlinx.coroutines.launch
import me.fungames.jfortniteparse.fileprovider.PakFileProvider
import me.fungames.jfortniteparse.ue4.FGuid
import me.fungames.jfortniteparse.ue4.locres.Locres
import me.fungames.jfortniteparse.ue4.pak.GameFile
import me.fungames.jfortniteparse.ue4.pak.PakFileReader
import me.fungames.jfortniteparse.ue4.versions.Ue4Version
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

@Suppress("EXPERIMENTAL_API_USAGE")
class ManifestFileProvider(val mountedBuild: MountedBuild, paksToSkip : List<String>, val localFilesFolder : File? = null, override var game : Ue4Version = Ue4Version.GAME_UE4_LATEST, concurrent : Boolean) : PakFileProvider() {
    override var defaultLocres : Locres? = null
    override val files = ConcurrentHashMap<String, GameFile>()
    override val keys = ConcurrentHashMap<FGuid, String>()
    override val mountedPaks = CopyOnWriteArrayList<PakFileReader>()
    override val requiredKeys = CopyOnWriteArrayList<FGuid>()
    override val unloadedPaks = CopyOnWriteArrayList<PakFileReader>()

    var concurrent : Boolean = concurrent
        set(value) {
            unloadedPaks.forEach { it.concurrent = value }
            mountedPaks.forEach { it.concurrent = value }
            field = value
        }

    init {
        mountedBuild.manifest.fileManifestList.filter { it.fileName.endsWith(".pak", true) &&
                it.fileName.startsWith("FortniteGame/Content/Paks") &&
                !paksToSkip.contains(it.fileName) }.forEach {
            try {
                val reader = PakFileReader(it.openPakArchive(mountedBuild, game))
                if (!reader.isEncrypted()) {
                    launch {
                        reader.readIndex()
                        reader.files.associateByTo(files, {file -> file.path.toLowerCase()})
                        mountedPaks.add(reader)
                    }
                } else {
                    unloadedPaks.add(reader)
                    if (!requiredKeys.contains(reader.pakInfo.encryptionKeyGuid))
                        requiredKeys.add(reader.pakInfo.encryptionKeyGuid)
                }
            } catch (e : Exception) {
                logger.error(e) { "Uncaught exception while open pak reader ${it.fileName.substringAfterLast("/")}" }
            }
        }
        localFilesFolder?.walkTopDown()?.forEach {
            if (it.isFile && it.extension.equals("pak", true)) {
                try {
                    val reader = PakFileReader(it, game.game)
                    if (!reader.isEncrypted()) {
                        launch {
                            reader.readIndex()
                            reader.files.associateByTo(files, {file -> file.path.toLowerCase()})
                            mountedPaks.add(reader)
                        }
                    } else {
                        unloadedPaks.add(reader)
                        if (!requiredKeys.contains(reader.pakInfo.encryptionKeyGuid))
                            requiredKeys.add(reader.pakInfo.encryptionKeyGuid)
                    }
                } catch (e : Exception) {
                    logger.error(e) { "Uncaught exception while opening local pak reader ${it.name}" }
                }
            }
        }
        this.concurrent = concurrent
    }
}