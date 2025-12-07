plugins {
  kotlin("jvm") version "2.1.0"
  `java-library`
  `maven-publish`
}

group = "com.ksdb.sql"
version = "1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  implementation(kotlin("reflect"))

  testImplementation(kotlin("test"))
  testImplementation("io.zonky.test:embedded-postgres:2.2.0")
}

tasks.test {
  useJUnitPlatform()
}
kotlin {
  jvmToolchain(23)
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      groupId = group as String?
      artifactId = "ksdb-sql"
      version = version
      from(components["kotlin"])
    }
  }
}