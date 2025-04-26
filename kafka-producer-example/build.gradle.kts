import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask

plugins {
    id("java")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.rolandsall"
version = "unspecified"


repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

tasks.register("generateAvro", GenerateAvroJavaTask::class) {
    group = "Generation"
    description = "Generates Avro Java files from Avro schemas."
    source("src/main/resources/schema_registry/avro")
    setOutputDir(file("src/main/java"))
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.5.1")

    implementation("io.confluent:kafka-avro-serializer:7.2.5") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "io.swagger", module = "swagger-annotations")
        exclude(group = "io.swagger", module = "swagger-core")
    }
    implementation("org.apache.avro:avro:1.11.1")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}