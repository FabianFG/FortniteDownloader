package me.fabianfg.fortnitedownloader

import kotlinx.coroutines.launch
import me.fungames.jfortniteparse.fileprovider.PakFileProvider
import me.fungames.jfortniteparse.ue4.assets.mappings.ReflectionTypeMappingsProvider
import me.fungames.jfortniteparse.ue4.assets.mappings.TypeMappingsProvider
import me.fungames.jfortniteparse.ue4.assets.mappings.UsmapTypeMappingsProvider
import me.fungames.jfortniteparse.ue4.io.*
import me.fungames.jfortniteparse.ue4.objects.core.misc.FGuid
import me.fungames.jfortniteparse.ue4.locres.Locres
import me.fungames.jfortniteparse.ue4.pak.GameFile
import me.fungames.jfortniteparse.ue4.pak.PakFileReader
import me.fungames.jfortniteparse.ue4.pak.reader.FPakFileArchive
import me.fungames.jfortniteparse.ue4.reader.FByteArchive
import me.fungames.jfortniteparse.ue4.versions.Ue4Version
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Class that implements a PakFileProvider using files from a manifest and MountedBuild
 * Also offers loading files from a local folder on disk
 *
 * This class only is thread-safe if you passed true for concurrent on creation
 *
 * This class is using FManifestPakArchive's for all files that have the .pak extension
 *
 * @param mountedBuild The MountedBuild used to read from files
 * @param paksToSkip Optional list of paks to be skipped during the loading process
 * @param localFilesFolder Optional folder on disk to search for additional pak files. If files in these are duplicates of the ones from the manifest they might override them
 * @param game The UE4 Version to use for parsing assets, defaults to the latest version
 * @param concurrent If true this file provider will be thread-safe by creating a copy of each FPakArchive before each operation. Use true if you plan to use any multithreading
 */
@Suppress("EXPERIMENTAL_API_USAGE")
class ManifestFileProvider(val mountedBuild: MountedBuild, mappingsProvider: TypeMappingsProvider = ReflectionTypeMappingsProvider(), paksToSkip : List<String> = emptyList(), val localFilesFolder : File? = null, override var game : Ue4Version = Ue4Version.GAME_UE4_LATEST, concurrent : Boolean) : PakFileProvider() {
    override val files = ConcurrentHashMap<String, GameFile>()
    override val keys = ConcurrentHashMap<FGuid, ByteArray>()
    override val mountedIoStoreReaders = CopyOnWriteArrayList<FIoStoreReaderImpl>()
    override val mountedPaks = CopyOnWriteArrayList<PakFileReader>()
    override val requiredKeys = CopyOnWriteArrayList<FGuid>()
    override val unloadedPaks = CopyOnWriteArrayList<PakFileReader>()

    var concurrent : Boolean = concurrent
        set(value) {
            unloadedPaks.forEach { it.concurrent = value }
            mountedPaks.forEach { it.concurrent = value }
            mountedIoStoreReaders.forEach { it.concurrent = value }
            field = value
        }

    init {
        this.mappingsProvider = mappingsProvider
        if (!globalDataLoaded) {
            val globalUtoc = mountedBuild.manifest.fileManifestList.firstOrNull { it.fileName == "FortniteGame/Content/Paks/global.utoc" }
            val globalUcas = mountedBuild.manifest.fileManifestList.firstOrNull { it.fileName == "FortniteGame/Content/Paks/global.ucas" }
            if (globalUtoc != null && globalUcas != null)
                loadGlobalData(globalUtoc, globalUcas)
        }
        mountedBuild.manifest.fileManifestList.filter { it.fileName.endsWith(".pak", true) &&
                it.fileName.startsWith("FortniteGame/Content/Paks", true) &&
                !paksToSkip.contains(it.fileName) }.forEach {
            try {
                val reader = PakFileReader(it.openPakArchive(mountedBuild, game))
                if (!reader.isEncrypted()) {
                    launch {
                        mount(reader)
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
                            mount(reader)
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

    override fun mount(reader: PakFileReader) {
        reader.readIndex()
        reader.files.associateByTo(files) { file -> file.path.lowercase(Locale.getDefault()) }
        mountedPaks.add(reader)

        if (globalDataLoaded) {
            val utocName = reader.fileName.replace(".pak", ".utoc")
            val ucasName = reader.fileName.replace(".pak", ".ucas")
            val utoc = mountedBuild.manifest.fileManifestList.firstOrNull { it.fileName == utocName }
            val ucas = mountedBuild.manifest.fileManifestList.firstOrNull { it.fileName == ucasName }
            if (utoc != null && ucas != null) {
                try {
                    val ioStoreReader = FIoStoreReaderImpl()
                    ioStoreReader.concurrent = reader.concurrent
                    val utocAr = utoc.openPakArchive(mountedBuild, game)
                    val utocByteAr = FByteArchive(utocAr.read(utocAr.size()))
                    ioStoreReader.initialize(utocByteAr, ucas.openPakArchive(mountedBuild, game), keys)
                    ioStoreReader.files.associateByTo(files) { it.path.lowercase(Locale.getDefault()) }
                    mountedIoStoreReaders.add(ioStoreReader)
                    globalPackageStore.value.onContainerMounted(FIoDispatcherMountedContainer(ioStoreReader.environment, ioStoreReader.containerId))
                    PakFileReader.logger.info("Mounted IoStore environment \"{}\"", ioStoreReader.environment.path)
                } catch (e: FIoStatusException) {
                    PakFileReader.logger.warn("Failed to mount IoStore environment \"{}\" [{}]", utocName, e.message)
                }
            }
        }

        mountListeners.forEach { it.onMount(reader) }
    }


    private fun loadGlobalData(utoc : FileManifest, ucas : FileManifest) {
        try {
            globalDataLoaded = true
            try {
                val ioStoreReader = FIoStoreReaderImpl()
                ioStoreReader.initialize(utoc.openPakArchive(mountedBuild, game), ucas.openPakArchive(mountedBuild, game), keys)
                mountedIoStoreReaders.add(ioStoreReader)
                PakFileReader.logger.info("Initialized I/O dispatcher")
            } catch (e: FIoStatusException) {
                PakFileReader.logger.error("Failed to mount I/O dispatcher global environment: '{}'", e.message)
            }
        } catch (e: FIoStatusException) {
            PakFileReader.logger.error("Failed to initialize I/O dispatcher: '{}'", e.message)
        }
    }
}