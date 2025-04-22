
plugins {
    java
    `java-library`
    id("io.spring.dependency-management")
}

java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

extra["springCloudVersion"] = "2024.0.0"
extra["springBootVersion"] = "3.4.2"
extra["testContainers"] = "1.20.4"

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${property("springBootVersion")}")
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        mavenBom("org.testcontainers:testcontainers-bom:${property("testContainers")}")
    }
}


tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
