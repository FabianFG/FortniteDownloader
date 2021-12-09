package me.fabianfg.fortnitedownloader

import me.fungames.jfortniteparse.fileprovider.PakFileProvider
import me.fungames.jfortniteparse.ue4.assets.mappings.ReflectionTypeMappingsProvider
import me.fungames.jfortniteparse.ue4.assets.mappings.TypeMappingsProvider
import me.fungames.jfortniteparse.ue4.io.*
import me.fungames.jfortniteparse.ue4.objects.core.misc.FGuid
import me.fungames.jfortniteparse.ue4.pak.GameFile
import me.fungames.jfortniteparse.ue4.pak.PakFileReader
import me.fungames.jfortniteparse.ue4.reader.FByteArchive
import me.fungames.jfortniteparse.ue4.versions.VersionContainer
import me.fungames.jfortniteparse.ue4.vfs.AbstractAesVfsReader
import java.io.File
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
open class ManifestFileProvider(val mountedBuild: MountedBuild, mappingsProvider: TypeMappingsProvider = ReflectionTypeMappingsProvider(), paksFilter: (fileName: String) -> Boolean = { true }, val localFilesFolder: File? = null, override var versions: VersionContainer = VersionContainer.DEFAULT, concurrent: Boolean): PakFileProvider() {
    override val files = ConcurrentHashMap<String, GameFile>()
    override val keys = ConcurrentHashMap<FGuid, ByteArray>()
    override val mountedPaks = CopyOnWriteArrayList<AbstractAesVfsReader>()
    override val requiredKeys = CopyOnWriteArrayList<FGuid>()
    override val unloadedPaks = CopyOnWriteArrayList<AbstractAesVfsReader>()

    var concurrent : Boolean = concurrent
        set(value) {
            unloadedPaks.forEach { it.concurrent = value }
            mountedPaks.forEach { it.concurrent = value }
            field = value
        }

    init {
        this.mappingsProvider = mappingsProvider
        for (it in mountedBuild.manifest.fileManifestList) {
            if (!it.fileName.startsWith("FortniteGame/Content/Paks", true) || !paksFilter(it.fileName)) continue
            val ext = it.fileName.substringAfterLast(".")
            if (ext == "pak") {
                try {
                    val reader = PakFileReader(it.openPakArchive(mountedBuild, versions))
                    reader.customEncryption = customEncryption
                    if (reader.isEncrypted()) {
                        requiredKeys.addIfAbsent(reader.pakInfo.encryptionKeyGuid)
                    }
                    unloadedPaks.add(reader)
                } catch (e: Exception) {
                    logger.error("Failed to open pak file \"${it.fileName}\"", e)
                }
            } else if (ext == "utoc") {
                val path = it.fileName.substringBeforeLast('.')
                try {
                    val utocAr = it.openPakArchive(mountedBuild, versions)
                    val utocByteAr = FByteArchive(utocAr.read(utocAr.size()))
                    val ucasName = "$path.ucas"
                    val ucas = mountedBuild.manifest.fileManifestList.firstOrNull { it.fileName == ucasName }
                    val reader = FIoStoreReaderImpl(utocByteAr, ucas!!.openPakArchive(mountedBuild, versions), ioStoreTocReadOptions)
                    reader.customEncryption = customEncryption
                    if (reader.isEncrypted()) {
                        requiredKeys.addIfAbsent(reader.encryptionKeyGuid)
                    }
                    unloadedPaks.add(reader)
                } catch (e: Exception) {
                    logger.error("Failed to open IoStore environment \"$path\"", e)
                }
            }
        }
        localFilesFolder?.let { scanFiles(it) }
        this.concurrent = concurrent
    }

    // Duplicate of the ones in DefaultFileProvider
    private fun scanFiles(folder: File) {
        for (file in folder.listFiles() ?: emptyArray()) {
            if (file.isDirectory) {
                scanFiles(file)
            } else if (file.isFile) {
                registerFile(file)
            }
        }
    }

    private fun registerFile(file: File) {
        val ext = file.extension.toLowerCase()
        if (ext == "pak") {
            try {
                val reader = PakFileReader(file, versions)
                reader.customEncryption = customEncryption
                if (reader.isEncrypted()) {
                    requiredKeys.addIfAbsent(reader.encryptionKeyGuid)
                }
                unloadedPaks.add(reader)
            } catch (e: Exception) {
                logger.error("Failed to open pak file \"${file.path}\"", e)
            }
        } else if (ext == "utoc") {
            val path = file.path.substringBeforeLast('.')
            try {
                val reader = FIoStoreReaderImpl(path, ioStoreTocReadOptions, versions)
                reader.customEncryption = customEncryption
                if (reader.isEncrypted()) {
                    requiredKeys.addIfAbsent(reader.encryptionKeyGuid)
                }
                unloadedPaks.add(reader)
            } catch (e: Exception) {
                logger.error("Failed to open IoStore environment \"$path\"", e)
            }
        }
        // We don't handle other file types
    }
}