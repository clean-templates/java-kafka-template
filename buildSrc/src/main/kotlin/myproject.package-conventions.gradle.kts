import custom.tasks.GenerateScript
import custom.tasks.MyAppExtension

plugins {
    java

}

val myApp = extensions.create<MyAppExtension>("MyApp")

val generateScript = tasks.register<GenerateScript>("generateScript") {
    mainClass.convention(myApp.mainClass)
    scriptFile.set(layout.buildDirectory.file("run.sh"))
}


val packageApp = tasks.register<Zip>("packageApp") {
    from(generateScript)
    from(tasks.jar) {
        into("libs")
    }
    from(configurations.runtimeClasspath) {
        into("libs")
    }

    destinationDirectory.set(layout.buildDirectory.dir("dist"))
    archiveFileName.set("MyApp.zip")
}
