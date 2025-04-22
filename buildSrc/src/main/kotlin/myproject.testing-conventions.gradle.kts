plugins {
    java
}

tasks.register<Test>("runUnitTests") {
    filter {
        excludeTestsMatching("*IntegrationTest")
        isFailOnNoMatchingTests = false
    }
}

tasks.register<Test>("runIntegrationTests") {
    filter {
        includeTestsMatching("*IntegrationTest")
        isFailOnNoMatchingTests = false
    }

}




