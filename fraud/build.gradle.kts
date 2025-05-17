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
    // Use consistent versions for Kafka components
    implementation("org.apache.kafka:kafka-clients:3.4.0")
    implementation("org.apache.kafka:kafka-streams:3.4.0")

    // Add KSQL dependencies
    implementation("io.confluent.ksql:ksqldb-api-client:7.2.5")
    implementation("io.confluent.ksql:ksqldb-udf:7.2.5")

    // Add SLF4J implementation
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.slf4j:slf4j-simple:1.7.36")

    implementation("io.confluent:kafka-avro-serializer:7.2.5") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "io.swagger", module = "swagger-annotations")
        exclude(group = "io.swagger", module = "swagger-core")
        exclude(group = "org.apache.kafka", module = "kafka-clients")
        exclude(group = "org.apache.kafka", module = "kafka-streams")
    }
    implementation("org.apache.avro:avro:1.11.1")

    // Jackson for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.14.2") // For Java 8 date/time support

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
