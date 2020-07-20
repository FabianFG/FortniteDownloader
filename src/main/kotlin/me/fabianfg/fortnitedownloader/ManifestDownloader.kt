package me.fabianfg.fortnitedownloader

import me.fungames.fortnite.api.FortniteApi
import me.fungames.fortnite.api.exceptions.EpicErrorException
import me.fungames.fortnite.api.model.BuildResponse
import me.fungames.fortnite.api.model.ClientDetails
import me.fungames.fortnite.api.model.EpicError
import me.fungames.fortnite.api.model.ManifestInfoResponse
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File

/**
 * Singleton to download manifests
 */
object ManifestLoader {
    @JvmStatic
    private val httpClient = OkHttpClient()

    /**
     * Download and parse a manifest from the launcher's response
     * @param manifestInfo The launcher's ManifestInfoResponse
     * @return The downloaded and parsed manifest
     */
    @JvmStatic
    fun downloadManifest(manifestInfo: ManifestInfoResponse) : Manifest {
        var url = "${manifestInfo.items.MANIFEST.distribution}${manifestInfo.items.MANIFEST.path}"
        val query : String? = manifestInfo.items.MANIFEST.signature
        if (query != null)
            url += "?$query"
        return downloadManifest(url)
    }

    /**
     * Download a manifest from the given url
     * @param url The url to download the file from and to determine the cloud dir
     * @return The downloaded and parsed manifest
     */
    @JvmStatic
    fun downloadManifest(url : String): Manifest {
        val request = Request.Builder().url(url).get().build()
        val manifestResponse = httpClient.newCall(request).execute()
        require(manifestResponse.isSuccessful) {"${manifestResponse.code} : ${manifestResponse.message}"}
        return Manifest.parse(
            manifestResponse.body!!.bytes(),
            url
        )
    }

    /**
     * Parse a manifest with the bytes from the passed file and the url as cloud dir
     * @param file The file to use as input
     * @param url The url where this file was originally download from. Can be either the cloud dir or the entire manifest url
     * @return The parsed manifest
     */
    @JvmStatic
    fun loadManifest(file : File, url : String) : Manifest =
        Manifest.parse(file.readBytes(), url)


    /**
     * Download and parse a manifest from the launcher's response from the v2 endpoint
     * @param manifestInfo The launcher's BuildResponse
     * @return The downloaded and parsed manifest
     */
    @JvmStatic
    fun downloadManifest(manifestInfo: BuildResponse) : Manifest {
        require(manifestInfo.elements.isNotEmpty()) {"BuildResponse does not contain any element"}
        val element = manifestInfo.elements.first()
        require(element.manifests.isNotEmpty()) {"BuildInfo does not contain any manifest"}
        val manifest = element.manifests.first()
        var url = manifest.uri.toHttpUrl()
        val queryParams = manifest.queryParams
        if (queryParams != null) {
            val builder = url.newBuilder()
            queryParams.forEach { builder.addQueryParameter(it.name, it.value) }
            url = builder.build()
        }
        val request = Request.Builder().url(url).build()
        val manifestResponse = httpClient.newCall(request).execute()
        require(manifestResponse.isSuccessful) {"${manifestResponse.code} : ${manifestResponse.message}"}
        return Manifest.parse(
            manifestResponse.body!!.bytes(),
            manifest.uri
        )
    }
}

/**
 * Class to query versions of games and download their manifests
 */
class LauncherManifestDownloader {
    companion object {
        private const val LAUNCHER_TOKEN =  "MzRhMDJjZjhmNDQxNGUyOWIxNTkyMTg3NmRhMzZmOWE6ZGFhZmJjY2M3Mzc3NDUwMzlkZmZlNTNkOTRmYzc2Y2Y"
    }
    private val api = FortniteApi.Builder().loginAsUser(false).clientToken(LAUNCHER_TOKEN).buildAndLogin()

    /**
     * Get the current manifest info for the passed platform and game
     * @param platform The platform to use
     * @param game The game to download the info of
     * @return The downloaded response from the launcher
     */
    fun getManifestInfo(platform: Platform, game: Game) : ManifestInfoResponse {
        if (System.currentTimeMillis() >= api.accountExpiresAtMillis)
            api.loginClientCredentials()
        val response = api.launcherPublicService.getManifest(platform.name, game.id, game.name, game.label).execute()
        if (!response.isSuccessful)
            throw EpicErrorException(EpicError.parse(response))
        return response.body()!!
    }

    /**
     * Get the current manifest info using the v2 endpoint for the passed platform and game
     * @param platform The platform to use
     * @param game The game to download the info of
     * @param clientDetails The details of your client
     * @return The downloaded response from the launcher
     */
    fun getMobileManifestInfo(platform: Platform, game: Game, clientDetails: ClientDetails) : BuildResponse {
        if (System.currentTimeMillis() >= api.accountExpiresAtMillis)
            api.loginClientCredentials()
        val response = api.launcherPublicService.getItemBuild(platform.name, game.id, game.name, game.label, clientDetails).execute()
        if (!response.isSuccessful)
            throw EpicErrorException(EpicError.parse(response))
        val elements = response.body()!!.elements.first().manifests.filter { !it.uri.toHttpUrl().host.endsWith("epicgames.com") }
        return BuildResponse(response.body()!!.buildStatuses, listOf(response.body()!!.elements.first().copy(manifests = elements)))
    }

    /**
     * Downloads the current manifest info for passed platform and game and downloads it
     * @param platform The platform to use
     * @param game The game to download the info and manifest of
     * @return a pair of both, the launcher's info response and the downloaded and parse manifest
     */
    fun downloadManifest(platform: Platform, game: Game): Pair<ManifestInfoResponse, Manifest> {
        val manifestInfo = getManifestInfo(platform, game)
        return manifestInfo to downloadManifest(manifestInfo)
    }
    /**
     * Downloads the current manifest info using the v2 endpoint for passed platform and game and downloads it
     * @param platform The platform to use
     * @param game The game to download the info and manifest of
     * @param clientDetails The details of your client
     * @return a pair of both, the launcher's info response and the downloaded and parse manifest
     */
    fun downloadMobileManifest(platform: Platform, game: Game, clientDetails: ClientDetails): Pair<BuildResponse, Manifest> {
        val manifestInfo = getMobileManifestInfo(platform, game, clientDetails)
        return manifestInfo to downloadManifest(manifestInfo)
    }

    /**
     * Shortcut that uses the ManifestLoader and downloads the passed info's manifest
     * @param manifestInfo The launcher's ManifestInfoResponse
     * @return The downloaded and parsed manifest
     *
     * @see ManifestLoader.downloadManifest
     */
    fun downloadManifest(manifestInfo : ManifestInfoResponse) =
        ManifestLoader.downloadManifest(manifestInfo)

    /**
     * Shortcut that uses the ManifestLoader and downloads the passed info's manifest
     * @param manifestInfo The launcher's BuildResponse from the v2 endpoint
     * @return The downloaded and parsed manifest
     *
     * @see ManifestLoader.downloadManifest
     */
    fun downloadManifest(manifestInfo : BuildResponse) =
        ManifestLoader.downloadManifest(manifestInfo)
}