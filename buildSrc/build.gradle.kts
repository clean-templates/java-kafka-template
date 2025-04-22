plugins {
    `kotlin-dsl`
}


extra["springBootVersion"] = "3.4.2"
extra["springDependencyManagementVersion"] = "1.1.7"

group = "com.rolandsall"
version = "1.0-SNAPSHOT"

    dependencies {
    implementation("org.springframework.boot:spring-boot-gradle-plugin:${property("springBootVersion")}")
    implementation("io.spring.gradle:dependency-management-plugin:${property("springDependencyManagementVersion")}")


}
