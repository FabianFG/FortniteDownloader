package me.fabianfg.fortnitedownloader

enum class Platform {
    Windows, Android, IOS
}

enum class Game(val id : String, val gameName : String, val label : String) {
    Fortnite("4fe75bbc5a674f4f9b356b5c90567da5", "Fortnite", "Live"),
    FortniteContentBuilds("5cb97847cee34581afdbc445400e2f77", "FortniteContentBuilds", "Live")
}