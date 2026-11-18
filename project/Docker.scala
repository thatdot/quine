import java.time.LocalDate

import scala.concurrent.duration.*
import scala.sys.process.*

import sbt.*
import sbt.Keys.{name, streams, target, version}
import sbt.io.IO
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.AssemblyPlugin
import sbtdocker.DockerKeys.{docker, dockerBuildArguments, dockerfile, imageNames}
import sbtdocker.staging.DefaultDockerfileProcessor
import sbtdocker.{DockerPlugin, Dockerfile, DockerfileLike, ImageName}

object Docker extends AutoPlugin {

  override def requires = AssemblyPlugin && DockerPlugin
  override def trigger = allRequirements

  object autoImport {
    // See https://github.com/marcuslonnberg/sbt-docker#pushing-an-image
    val dockerTags = SettingKey[Seq[String]]("docker-tags", "The tag names to push the docker image under")
    val dockerVolume = SettingKey[File]("docker-volume", "Path to where the app should save its data")
    val dockerJarTask = docker / taskKey[File]("The JAR file to include in the Docker image")
    val dockerDistDir = docker / taskKey[Option[File]](
      "Staged distribution with <name>.jar and lib/, copied to /opt/<name>/ in two layers; None for a self-contained JAR",
    )
    val dockerStage = docker / taskKey[File]("Stage docker context without building the image")
  }
  import autoImport.*

  // The image is a chiseled Ubuntu root filesystem plus the Amazon Corretto Java runtime. It contains only glibc,
  // libstdc++, libgcc_s and zlib, the en_US.UTF-8 and C.UTF-8 locales, /etc/passwd and /etc/group, bash (also
  // linked as /bin/sh), a set of command-line tools (see `toolSlices`) and the Java runtime. There is no apt,
  // dpkg or curl, so the image cannot install packages. curl and its libraries (OpenSSL, gnutls, krb5, sasl and
  // others) brought every OS CVE that was left after chiseling. There is no system CA bundle either: Java uses
  // the trust store of the Java runtime (lib/security/cacerts).

  // One value for the builder image and for the chisel release, so the packages always come from the Ubuntu
  // release that cut them. The Corretto runtime comes from an Amazon Linux image and needs only glibc and zlib.
  private val ubuntuRelease = "24.04"
  // Amazon Corretto 21.0.13.1.2, which has the fix for CVE-2026-114938. The tag is mutable, the digest is the pin.
  private val correttoTag = "21.0.13-al2023-headless"

  // Both digests are multi-arch index digests (linux/amd64 and linux/arm64), not per-architecture manifests.
  // Check a new digest with `docker buildx imagetools inspect <image>@<digest>` before pinning it.
  private val ubuntuDigest = "sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852"
  private val correttoDigest = "sha256:71b005271e4df1a2fb44f6d261bad4318e66aff82ea7541b4d2968a9a31357d7"

  private val chiselVersion = "v1.5.1"
  private val chiselSha384Amd64 =
    "89612f46de83e850aae11fca26a32a22cc2964891f2cc538211b906787d2b83e4496fa203a5074e06a9932f1fbe84a73"
  private val chiselSha384Arm64 =
    "5beb26c4ce29741493e7481139742b1af1b087eab043d102313d9a5caaf665169678db4c980a62098376841220f15577"

  // What the native libraries in the JARs need (checked with ldd on aarch64): RocksDB needs libstdc++ and
  // libgcc_s, and jffi, lmdbjava, snappy, zstd, lz4 and truffle need glibc only. Without libstdc++ and libgcc_s
  // the only symptom is "RocksDB native library could not be loaded".
  private val chiselSlices = Seq(
    "base-files_base",
    "base-files_bin",
    "base-files_lib",
    "base-files_tmp",
    "base-files_var",
    "base-files_release-info", // /etc/os-release, which scanners use to detect the OS
    "base-files_chisel",
    "base-passwd_data",
    "libc6_libs",
    // The C.UTF-8 locale, which the Temurin image also has, for a deployment that sets LANG or LC_ALL to it.
    // Ubuntu has no prebuilt en_US.UTF-8, so that locale is generated after the cut.
    "libc-bin_locale",
    "libstdc++6_libs",
    "libgcc-s1_libs",
    // Corretto links to the system zlib (libjli, libzip and libjava need libz.so.1). Temurin has its own copy.
    "zlib1g_libs",
    "bash_bins",
  ) ++ toolSlices

  // Tools for scripts, probes and work in a running container. Each one is here because its package and the
  // libraries it needs (libcap2, pcre2, libselinux) had no open CVE in a Trivy scan when it was added.
  // Left out because they had open CVEs or bring them: procps (ps and top need libsystemd), tar,
  // coreutils_bins (cp, wc, tee and sort come only in that slice, and sort needs OpenSSL), and
  // coreutils_mv-utility and sed_bins
  private lazy val toolSlices = Seq(
    "coreutils_directory-listing", // ls
    "coreutils_output-of-entire-files", // cat, base64
    "coreutils_delaying", // sleep
    "coreutils_env",
    "coreutils_working-context", // pwd, printenv
    "coreutils_system-context", // uname, nproc
    "coreutils_head",
    "coreutils_tail",
    "coreutils_cut",
    "coreutils_tr-utility",
    "coreutils_date",
    "coreutils_stat",
    "coreutils_id-utility",
    "coreutils_test",
    "coreutils_timeout",
    "coreutils_mkdir",
    "coreutils_mktemp",
    "coreutils_touch",
    "coreutils_rm-utility",
    "coreutils_rmdir",
    "coreutils_ln-utility",
    "coreutils_readlink",
    "coreutils_chmod",
    "coreutils_chown",
    "coreutils_basename",
    "coreutils_dirname",
    "grep_bins",
    "findutils_bins", // find, xargs
  )

  private val javaHome = "/opt/java/openjdk"

  private val installChisel =
    """set -eu; \
      |    archive="chisel_${CHISEL_VERSION}_linux_${TARGETARCH}.tar.gz"; \
      |    case "$TARGETARCH" in \
      |      amd64) sum="$CHISEL_SHA384_amd64" ;; \
      |      arm64) sum="$CHISEL_SHA384_arm64" ;; \
      |      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
      |    esac; \
      |    curl -fsSL --retry 3 -o "/tmp/$archive" "https://github.com/canonical/chisel/releases/download/${CHISEL_VERSION}/$archive"; \
      |    echo "$sum  /tmp/$archive" | sha384sum --check --strict; \
      |    tar -xzf "/tmp/$archive" -C /usr/local/bin chisel""".stripMargin

  // Cuts the root filesystem. Chisel fetches the packages from the Ubuntu archive with a 30 s timeout and does
  // not try again, so one slow answer from the archive would fail the whole image build. Each attempt starts
  // with an empty /rootfs, because a cut that failed can leave files behind.
  private val chiselCutAttempts = 3
  private lazy val cutRootfs =
    s"""{ attempt=1; \\
       |    until rm -rf /rootfs && mkdir /rootfs && chisel cut --release ubuntu-$ubuntuRelease --root /rootfs ${chiselSlices
      .mkString(" ")}; do \\
       |      if [ "$$attempt" -ge $chiselCutAttempts ]; then echo "chisel cut failed $chiselCutAttempts times" >&2; exit 1; fi; \\
       |      echo "chisel cut failed, attempt $$attempt of $chiselCutAttempts, next attempt in 10 s" >&2; \\
       |      attempt=$$((attempt + 1)); sleep 10; \\
       |    done; }""".stripMargin

  // Declares the installed packages to vulnerability scanners. Chisel records them only in its own manifest
  // (/var/lib/chisel/manifest.wall), which Trivy does not read: without these records a scan reports 0 OS
  // packages, which hides real CVEs. The records use the distroless layout, one file per package in
  // /var/lib/dpkg/status.d/. The Source field matters: Ubuntu files CVEs under the source package name.
  //
  // Every step stops the build when it fails. There is no pipeline into the loop, because the shell (dash) has
  // no pipefail, and the final count must equal the number of packages in the manifest.
  private val writeDpkgRecords =
    """set -eu; \
      |    records=/rootfs/var/lib/dpkg/status.d; \
      |    mkdir -p "$records"; \
      |    zstd -dc /rootfs/var/lib/chisel/manifest.wall > /tmp/manifest.jsonl; \
      |    jq -r 'select(.kind == "package") | "\(.name) \(.version) \(.arch)"' /tmp/manifest.jsonl > /tmp/packages; \
      |    [ -s /tmp/packages ] || { echo "The chisel manifest lists no packages" >&2; exit 1; }; \
      |    while read -r name version arch; do \
      |      record=$(apt-cache show "$name=$version") || { echo "No apt record for $name=$version" >&2; exit 1; }; \
      |      [ -n "$record" ] || { echo "Empty apt record for $name=$version" >&2; exit 1; }; \
      |      src=$(printf '%s\n' "$record" | awk '/^Source:/ { print $2; exit }'); \
      |      { echo "Package: $name"; \
      |        echo "Status: install ok installed"; \
      |        echo "Architecture: $arch"; \
      |        echo "Version: $version"; \
      |        if [ -n "$src" ]; then echo "Source: $src"; fi; \
      |      } > "$records/$name"; \
      |    done < /tmp/packages; \
      |    expected=$(wc -l < /tmp/packages); \
      |    actual=$(ls "$records" | wc -l); \
      |    [ "$actual" -eq "$expected" ] || { echo "Wrote $actual dpkg records for $expected packages" >&2; exit 1; }; \
      |    echo "Wrote $actual dpkg records"""".stripMargin

  // Healthy when GET /api/v2/system/liveness answers with a 2xx status. The image has no curl, so bash sends the
  // request through its /dev/tcp redirection, as plain HTTP to localhost. The status line is
  // "HTTP/1.1 <code> <reason>"; `read` puts the code in $code, and when there is no reason phrase the code ends
  // with a carriage return, which is removed. Docker stops the check after --timeout.
  // Set QUINE_HEALTHCHECK_PORT when the web server does not listen on 8080.
  private val livenessProbe =
    """exec 3<>"/dev/tcp/127.0.0.1/${QUINE_HEALTHCHECK_PORT:-8080}"""" +
    """ && printf 'GET /api/v2/system/liveness HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3""" +
    """ && read -r _ code _ <&3 && code=${code%$'\r'} && [[ $code == 2[0-9][0-9] ]]"""

  override lazy val projectSettings = Seq(
    dockerVolume := file("/var/quine"),
    dockerTags := sys.props.get("docker.tag").fold(Seq(version.value, "latest"))(Seq(_)),
    docker / imageNames := dockerTags.value.map(t =>
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t)),
    ),
    // Enforce Docker image format rather than OCI format (the Podman default), enabling HEALTHCHECK
    docker / dockerBuildArguments := Map("format" -> "docker"),
    // Quine OSS ships its fat `assembly` JAR; `ThinDist.settings` sets `dockerDistDir` for the commercial products
    docker / dockerJarTask := assembly.value,
    docker / dockerDistDir := None,
    docker / dockerfile := {
      val product = name.value
      val dist: Option[sbt.File] = dockerDistDir.value
      val assemblyJar: sbt.File = dockerJarTask.value
      val jar: sbt.File = dist.fold(assemblyJar)(_ / s"$product.jar")
      val lib: Option[sbt.File] = dist.map(_ / "lib")
      val appDir = if (dist.isDefined) s"/opt/$product" else ""
      val jarPath = s"$appDir/${jar.name}"
      val jmxPrometheusJarName = "jmx_prometheus_javaagent.jar"
      val temp = IO.createTemporaryDirectory
      val jmxPrometheusFile: sbt.File = temp / "jmx_prometheus_javaagent.jar"
      url(
        "https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar",
      ) #> jmxPrometheusFile !
      val exporterYamlName = "exporter.yaml"
      val exporterYamlFile = temp / exporterYamlName
      IO.append(exporterYamlFile, "rules:\n- pattern: \".*\"")
      val exporterYamlPath = "/" + exporterYamlName
      // The date changes the text of the instructions that fetch packages, so the layer cache cannot serve stale
      // apt lists or stale package versions. Both instructions carry it: chisel cuts current versions, and the
      // dpkg records below need apt lists that contain those versions.
      val refreshDate = LocalDate.now
      val base = new Dockerfile {
        // Stage 1: cut the root filesystem with chisel. This stage is not shipped, so it keeps the apt lists:
        // they give the source package names for the dpkg records.
        from(s"ubuntu:$ubuntuRelease@$ubuntuDigest AS rootfs")
        arg("TARGETARCH")
        arg("CHISEL_VERSION", Some(chiselVersion))
        arg("CHISEL_SHA384_amd64", Some(chiselSha384Amd64))
        arg("CHISEL_SHA384_arm64", Some(chiselSha384Arm64))
        runRaw(
          s"echo apt-refresh=$refreshDate && apt-get -o Acquire::Retries=3 update" +
          " && apt-get -o Acquire::Retries=3 install -y --no-install-recommends ca-certificates curl jq locales zstd",
        )
        runRaw(installChisel)
        runRaw(
          s"echo chisel-refresh=$refreshDate" +
          s" && $cutRootfs" +
          " && ln -s bash /rootfs/usr/bin/sh" +
          // The same link as in the Temurin image. Without it the default time zone of the JVM is GMT, not
          // Etc/UTC, and zoned datetimes print as "...Z[GMT]". The JVM reads only the name in the link, so the
          // target does not need to exist (the image has no tzdata). The TZ variable still overrides it.
          " && ln -s /usr/share/zoneinfo/Etc/UTC /rootfs/etc/localtime" +
          // The locale that LANG names below, the same as in the Temurin image. Without these files glibc uses
          // the C locale, and the JVM then uses ASCII for standard output and for file names: non-ASCII text
          // prints as "?" and a file with a non-ASCII name cannot be opened.
          " && mkdir -p /rootfs/usr/lib/locale" +
          " && localedef --no-archive -i en_US -f UTF-8 /rootfs/usr/lib/locale/en_US.UTF-8",
        )
        runRaw(writeDpkgRecords)

        // Stage 2: the Java runtime. In the Corretto image JAVA_HOME is a symbolic link, and `cacerts` is a
        // link to the trust store of Amazon Linux. `cp -rL` makes both regular files and directories, so the
        // copy below keeps TLS and `keytool -cacerts` working.
        from(s"amazoncorretto:$correttoTag@$correttoDigest AS jre")
        runRaw(s"mkdir -p /opt/java && cp -rL /usr/lib/jvm/java-21-amazon-corretto $javaHome")

        // Stage 3: the runtime image
        from("scratch")
        customInstruction("COPY", "--from=rootfs /rootfs /")
        customInstruction("COPY", s"--from=jre $javaHome $javaHome")
        env(
          "JAVA_HOME" -> javaHome,
          "PATH" -> s"$javaHome/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
          // The values of the Temurin image. The image has the en_US.UTF-8 and C.UTF-8 locales.
          "LANG" -> "en_US.UTF-8",
          "LANGUAGE" -> "en_US:en",
          "LC_ALL" -> "en_US.UTF-8",
        )
        healthCheck(
          Seq("/bin/bash", "-c", livenessProbe),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds),
        )
        expose(7626, 8080)
        env("QUINE_DATA", dockerVolume.value.getPath)
        volume("$QUINE_DATA")
        // Dependencies first, so a first-party change invalidates only the product JAR's layer
        lib.foreach(copy(_, s"$appDir/lib"))
        copy(jar, jarPath)
        copy(jmxPrometheusFile, jmxPrometheusJarName)
        copy(exporterYamlFile, exporterYamlPath)
      }
      base
        .entryPoint(
          "java",
          "-XX:+AlwaysPreTouch",
          "-XX:+UseParallelGC",
          "-XX:InitialRAMPercentage=40.0",
          "-XX:MaxRAMPercentage=80.0",
          "-jar",
          jarPath,
        )
    },
    dockerStage := {
      val log = streams.value.log
      val stageDir = target.value / "docker"
      val df = (docker / dockerfile).value.asInstanceOf[DockerfileLike]

      // Use sbt-docker's internal staging processor
      val staged = DefaultDockerfileProcessor(df, stageDir)

      // Clean and create stage directory
      IO.delete(stageDir)
      IO.createDirectory(stageDir)

      // Write Dockerfile
      IO.write(stageDir / "Dockerfile", staged.instructionsString)

      // Copy all staged files
      staged.stageFiles.foreach { case (source, dest) =>
        source.stage(dest)
      }

      log.info(s"Docker context staged to: $stageDir")
      stageDir
    },
  )
}
