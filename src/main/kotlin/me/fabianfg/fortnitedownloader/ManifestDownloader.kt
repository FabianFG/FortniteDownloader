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

object ManifestLoader {
    private val httpClient = OkHttpClient()

    fun downloadManifest(manifestInfo: ManifestInfoResponse) : Manifest {
        var url = "${manifestInfo.items.MANIFEST.distribution}${manifestInfo.items.MANIFEST.path}"
        val query : String? = manifestInfo.items.MANIFEST.signature
        if (query != null)
            url += "?$query"
        return downloadManifest(url)
    }
    fun downloadManifest(url : String): Manifest {
        val request = Request.Builder().url(url).get().build()
        val manifestResponse = httpClient.newCall(request).execute()
        require(manifestResponse.isSuccessful) {"${manifestResponse.code} : ${manifestResponse.message}"}
        return Manifest.parse(
            manifestResponse.body!!.bytes(),
            url
        )
    }

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

class LauncherManifestDownloader {
    companion object {
        private const val LAUNCHER_TOKEN =  "MzRhMDJjZjhmNDQxNGUyOWIxNTkyMTg3NmRhMzZmOWE6ZGFhZmJjY2M3Mzc3NDUwMzlkZmZlNTNkOTRmYzc2Y2Y"
    }
    private val api = FortniteApi.Builder().loginAsUser(false).clientToken(LAUNCHER_TOKEN).buildAndLogin()


    fun getManifestInfo(platform: Platform, game: Game) : ManifestInfoResponse {
        if (System.currentTimeMillis() >= api.accountExpiresAtMillis)
            api.loginClientCredentials()
        val response = api.launcherPublicService.getManifest(platform.name, game.id, game.name, game.label).execute()
        if (!response.isSuccessful)
            throw EpicErrorException(EpicError.parse(response))
        return response.body()!!
    }

    fun getMobileManifestInfo(platform: Platform, game: Game, clientDetails: ClientDetails) : BuildResponse {
        if (System.currentTimeMillis() >= api.accountExpiresAtMillis)
            api.loginClientCredentials()
        val response = api.launcherPublicService.getItemBuild(platform.name, game.id, game.name, game.label, clientDetails).execute()
        if (!response.isSuccessful)
            throw EpicErrorException(EpicError.parse(response))
        val elements = response.body()!!.elements.first().manifests.filter { !it.uri.toHttpUrl().host.endsWith("epicgames.com") }
        return BuildResponse(response.body()!!.buildStatuses, listOf(response.body()!!.elements.first().copy(manifests = elements)))
    }

    fun downloadManifest(platform: Platform, game: Game): Pair<ManifestInfoResponse, Manifest> {
        val manifestInfo = getManifestInfo(platform, game)
        return manifestInfo to downloadManifest(manifestInfo)
    }
    fun downloadMobileManifest(platform: Platform, game: Game, clientDetails: ClientDetails): Pair<BuildResponse, Manifest> {
        val manifestInfo = getMobileManifestInfo(platform, game, clientDetails)
        return manifestInfo to downloadManifest(manifestInfo)
    }
    fun downloadManifest(manifestInfo : ManifestInfoResponse) =
        ManifestLoader.downloadManifest(manifestInfo)
    fun downloadManifest(manifestInfo : BuildResponse) =
        ManifestLoader.downloadManifest(manifestInfo)
}