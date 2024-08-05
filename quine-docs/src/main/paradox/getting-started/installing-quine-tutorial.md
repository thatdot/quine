---
description: Getting Started - Installing Quine
---

# Installing Quine

Quine is a key participant in a streaming event data pipeline that consumes data, builds it into a graph structure, runs computation on that graph to answer questions or compute results, and then stream them out. Quine combines the real-time event processing capabilities of systems like Flink and ksqlDB with the graph data structure found in graph databases like Neo4j and Tigergraph.

There are multiple ways for you to install Quine, select the one that is best for your environment from the list below.

| Method                          | Description                                                                                                                                                                                  |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| @ref:[**Docker Container**](#docker-container)                | If you already have Docker, this is the quickest way to get up and running with the minimum setup time and impact to your environment.                                                       |
| @ref:[**Download Java distribution file**](#distribution-file) | Download the `jar` file to run locally on your laptop or development server. Running the `jar` file locally is the most flexible way to evaluate Quine.           |
| @ref:[**Build from source**](#open-source)               | Need to do a deeper evaluation of how Quine operates? Clone the open source repository from GitHub and dig in!                                                                               |
| @ref:[**Cloud hosted by thatDot**](#cloud-hosted-saas)         | Are you interested in the [Quine Enterprise features](https://www.thatdot.com/product/pricing)? Contact our sales team to set up an evaluation environment in your cloud provider of choice. |

Follow the steps below to install Quine for use in your environment.

---

## Docker Container

Docker allows you to install a containerized version of Quine for evaluation.

### Prerequisites

* **Host Environment** - You need a @link:[Mac](https://docs.docker.com/desktop/install/mac-install/) { open=new }, @link:[Windows](https://docs.docker.com/desktop/install/windows-install/) { open=new }, or @link:[Unix](https://docs.docker.com/desktop/install/linux-install/) { open=new } host server to run Docker. Please be sure that your host meets the system requirements outlined in the proceeding links.
* **Docker Desktop** - Instructions on how to install Docker can be found on the [official Docker website](https://docs.docker.com/get-docker/).

### Install and start the Quine container

The @link:[Quine Docker image](https://hub.docker.com/r/thatdot/quine) { open=new } is distributed via Docker Hub and can be installed and launched with a single command.

1. Open a terminal window
2. With the Docker desktop application running, issue the following command

```shell
docker run -p 8080:8080 thatdot/quine
```

If successful, you will see a message similar to the following appear in the terminal.

```text
Unable to find image 'thatdot/quine:latest' locally
latest: Pulling from thatdot/quine
2408cc74d12b: Pull complete
3d4177d25912: Pull complete
84eef58e1007: Pull complete
7d414c479da8: Pull complete
ac0978c82c5c: Pull complete
5e38591d5629: Pull complete
Digest: sha256:8200a2ea46aaa021865cfa7e843c65bb3f6dded4d00329217800f1a26df36e14
Status: Downloaded newer image for thatdot/quine:latest
WARNING: The requested images platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
Graph is ready
Quine web server available at http://127.0.0.1:8080
```

Verify that Quine is operating using the `admin/build-info` API endpoint.

```shell
❯ curl -s "http://127.0.0.1:8080/api/v1/admin/build-info" | jq '.'
{
  "version": "1.3.2",
  "gitCommit": "6f8bb1b3a308d9c90cc71a2328858907ec341e74",
  "gitCommitDate": "2022-08-10T11:01:51-0700",
  "javaVersion": "OpenJDK 64-Bit Server VM 17.0.2 (Azul Systems, Inc.)",
  "persistenceWriteVersion": "12.0.0"
}
```

You can connect to the Quine exploration UI by entering `http://127.0.0.1:8080` into your browser.

### Shutdown Quine

POST to the `admin/shutdown` endpoint to gracefully shutdown Quine and stop the Docker container.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/admin/shutdown"
```

Both Quine and the Docker container will shutdown and exit. If successful, you will see a message similar to the following appear in the terminal and the container will have a status of `exited` in the Docker desktop.

```text
Quine is shutting down...
Shutdown complete
```

---

## Distribution File

Using a distribution file to run Quine locally provides the most flexibility ...

* Evaluate Quine using your hardware of choice
* Make changes to the Quine @link:[configuration](https://docs.quine.io/reference/configuration.html) { open=new } file
* Launch Quine @link:[recipes](https://docs.quine.io/reference/recipe-ref-manual.html) { open=new }
* Benchmark Quine's performance running different versions of Java

### Prerequisites

You will need the following in order to run Quine in your environment.

* **Java JRE version 11 or greater** - instructions for how to install Java can be found on the @link:[official Java web site](https://www.java.com/en/download/help/download_options.html) { open=new }.

### Download the Quine Distribution File

Quine is distributed as a pre-built Java `jar` package that you download and run on your local laptop or server.

1. Download the `jar` file from the `quine.io` @link:[download](https://quine.io/download) { open=new } page.
2. Store the `jar` file in a working directory

We recommend storing the Quine `jar` file in the same directory that you plan to use during your evaluation and development.

On a Mac, this process would look similar to this.

* Selecting the `JAR DOWNLOAD` button prompted the browser to download the latest version of Quine and store it in the `~/Downloads` directory.
* Once the download completes, issue the following commands to move Quine into a directory that you can use for evaluation and testing.

```shell
❯ mkdir gettingStarted
❯ cd gettingStarted
❯ mv ~/Downloads/quine-1.3.2.jar .
❯ ls -l
total 460088
-rw-r--r--@ 1 quine  staff  220704180 Aug 25 09:22 quine-1.3.2.jar
```

### Start Quine

Once you have the Quine package stored locally, you are ready to launch Quine for the first time.

To launch Quine, issue the following command.

```shell
❯ java -jar quine-1.3.2.jar
```

If successful, you will see a message similar to the following appear in the terminal.

```text
Graph is ready
Quine web server available at http://127.0.0.1:8080
```

Verify that Quine is operating using the `admin/build-info` API endpoint.

```shell
curl -s "http://127.0.0.1:8080/api/v1/admin/build-info" | jq '.'
{
  "version": "1.3.2",
  "gitCommit": "6f8bb1b3a308d9c90cc71a2328858907ec341e74",
  "gitCommitDate": "2022-08-10T11:01:51-0700",
  "javaVersion": "OpenJDK 64-Bit Server VM 17.0.2 (Azul Systems, Inc.)",
  "persistenceWriteVersion": "12.0.0"
}
```

You can connect to the Quine exploration UI by entering `http://127.0.0.1:8080` into your browser.

### Shutdown Quine

You can stop Quine at any time by either typing `CTRL-c` into the terminal window or gracefully shutting down by issuing a POST to the @link:[admin/shutdown](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-admin-shutdown/post) { open=new } endpoint.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/admin/shutdown"
```

---

## Open Source

Quine is an open source project and can be built from source code.

Build Quine from source code if ...

* You want to review the code as part of your evaluation
* You are interested in contributing to the Quine open source project
* You need an enhancement or issue resolution that was delivered between releases

### Prerequisites

Quine is written in Scala and Node JS. Please review the [README.md](https://github.com/thatdot/quine) file in the GitHub repository for most up to date requirements to build Quine from source.

* Java JDK 11 or greater
* Scala SBT version 1.7.1 or greater
* Node version 16
* Yarn version 0.22.0 or greater

### Clone the Quine repository from GitHub

The Quine open source project is managed on GitHub, and the repo is located at @link:[https://github.com/thatdot/quine](https://github.com/thatdot/quine) { open=new }.

```shell
❯ mkdir quineEvaluation
❯ cd quineEvaluation
❯ git clone git@github.com:thatdot/quine.git
Cloning into 'quine'...
```

### Build Quine

```shell
❯ cd quine
❯ nvm use 16
Now using node v16.15.1 (npm v8.11.0)
❯ sbt quine/assembly
[info] welcome to sbt 1.7.1 (Homebrew Java 11.0.16.1)
[info] loading settings for project quine-build from plugins.sbt
... <build details removed> ...
[success] Total time: 78 s (01:18), completed Aug 25, 2022, 9:54:42 AM
```

If successful, the `sbt` tool will configure your environment, build, and package Quine into a `jar` file located in the target directory.

```shell
❯ find . -name "quine-*.jar"
./quine/target/scala-2.12/quine-assembly-1.3.2+n.jar
```

### Start Quine

Once that you have successfully built and assembled the Quine `jar` file, we recommend copying that file into a working directory to make it easier to evaluate Quine.

```shell
❯ cp $(find . -name "quine-assembly-*.jar") ..
❯ cd ..
❯ ls
quine/  quine-assembly-1.3.2+n.jar
```

Then you can launch Quine in the same way that you would launch the distribution file.

```shell
❯ java -jar quine-assembly-1.3.2+n.jar
```

If successful, you will see a message similar to the following appear in the terminal.

```text
Graph is ready
Quine web server available at http://127.0.0.1:8080
```

Alternatively, you can run Quine directly from the source code with `sbt quine/run`.

```text
❯ sbt quine/run
[info] welcome to sbt 1.7.1 (Homebrew Java 11.0.16.1)
[info] loading settings for project quine-build from plugins.sbt ...
...
[info] running com.thatdot.quine.app.Main
Graph is ready
Quine web server available at http://127.0.0.1:8080
```

Regardless of the method that you choose to launch Quine, you can verify that it is operating using the `admin/build-info` API endpoint.

```shell
❯ curl -s "http://127.0.0.1:8080/api/v1/admin/build-info" | jq '.'
{
  "version": "1.3.2+n",
  "gitCommit": "45dc9789b344b5c282cab227c560c45bc1a883b5",
  "gitCommitDate": "2022-08-24T19:29:11+0000",
  "javaVersion": "OpenJDK 64-Bit Server VM 11.0.16.1 (Homebrew)",
  "persistenceWriteVersion": "12.0.0"
}
```

You can connect to the Quine exploration UI by entering `http://127.0.0.1:8080` into your browser.

### Shutdown Quine

You can stop Quine at any time by either typing `CTRL-c` into the terminal window or gracefully shutting down by issuing a POST to the @link:[admin/shutdown](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-admin-shutdown/post) { open=new } endpoint.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/admin/shutdown"
```

---

## Cloud Hosted / SaaS

Please contact the thatDot sales team using the link below to set up a call if you are interested in the Quine-Enterprise edition or a SaaS hosted option.

@link:[Contact Sales](https://www.thatdot.com/contact-us/pricing-request) { open=new }
