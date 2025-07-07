This quick start will guide you through deploying the proxy.

Kroxylicious is a Java application based on [Netty](https://netty.io/), which means it will run anywhere you can run a JVM.

# Getting started

## Prerequisites

### Java

To get started deploying Kroxylicious, you will need to install a Java Runtime Environment (JRE) with minimum version 17. 
This does not come included with Kroxylicious.

Some operating systems come with a JRE already installed. You can check what Java version you have installed by running the following command:

```shell
java -version
```

{% capture jre_note %}
If you get an error or the command doesn't return anything, you may not have a JRE installed, or your JRE may not have been correctly added to your system's `PATH` variable.
{% endcapture %}

{% include bs-alert.html type="primary" icon="info-circle-fill" content=jre_note %}

### Apache Kafka®

You will also need a running Apache Kafka® cluster for Kroxylicious to proxy. 
The official Apache Kafka® [quick start](https://kafka.apache.org/documentation/#quickstart) has instructions for setting up a local bare metal cluster.

Once your cluster is set up, the cluster bootstrap address used by Kroxylicious can be changed in the configuration YAML file (see the [**Configure**](#step-2-configure-the-proxy) section below).

## Step 1: Download and install the proxy

Kroxylicious can be downloaded from the [releases](https://github.com/kroxylicious/kroxylicious/releases) page of the Kroxylicious GitHub repository, or from Maven Central.

Download [`kroxylicious-app-{{ page.version }}-bin.zip`](https://github.com/kroxylicious/kroxylicious/releases/download/v{{ page.version }}/kroxylicious-app-{{ page.version }}-bin.zip) from the [GitHub release page](https://github.com/kroxylicious/kroxylicious/releases/tag/v{{ page.version }}).
Optionally, verify the contents of the package with the attached [`kroxylicious-app-{{ page.version }}-bin.zip.asc`](https://github.com/kroxylicious/kroxylicious/releases/download/v{{ page.version }}/kroxylicious-app-{{ page.version }}-bin.zip.asc) file.

{% capture os_archive_note %}
If you're trying Kroxylicious out on Linux or macOS, you may find the `.tar.gz` format easier to work with. 
We're using the `.zip` files in this quick start for cross-platform compatibility, but we recommend you use whichever format you're most familiar with.
{% endcapture %}

{% include bs-alert.html type="primary" icon="info-circle-fill" content=os_archive_note %}

Extract the downloaded Kroxylicious Zip file into the directory you would like to install Kroxylicious in.
Ensure the `kroxylicious-start.sh` and `run-java.sh` files in the `bin/` directory within the extracted folder have at least read and execute (`r-x`) permissions for the owner.

## Step 2: Configure the proxy

Kroxylicious is configured with YAML. 
From the configuration file you can specify how Kroxylicious presents each Apache Kafka® broker to clients, where Kroxylicious will locate the Apache Kafka® cluster(s) to be proxied, and which filters Kroxylicious should use along with any configuration for those filters.

An example configuration file can be found in the `config/` directory of the extracted Kroxylicious folder, which you can either modify or use as reference for creating your own configuration file.

For this quickstart we will use Kroxylicious in Port-Per-Broker configuration, and assume that both your Apache Kafka® cluster and clients are running on your local machine and using their default configuration. 
This means we can use the example proxy config file that comes with Kroxylicious.

If your machine uses a non-standard port configuration, or if you have used custom settings for your Apache Kafka® cluster (or if your cluster is running on a different machine) you will need to adjust your Kroxylicious configuration accordingly. 
More information about configuring Kroxylicious can be found in the [documentation](/documentation/).

## Step 3: Start the proxy

From within the extracted Kroxylicious folder, run the following command:

```shell
./bin/kroxylicious-start.sh --config config/example-proxy-config.yml
```

To use your own configuration file instead of the example, just replace the file path after `--config`.

## Step 4: Configure Kafka clients to use it

Your client(s) will need to point to the proxy (using the configured address) rather than directly at the Apache Kafka® cluster.

[//]: # (====================================================================)
[//]: # (START - Use Section Examples Tabbed Card)
[//]: # (====================================================================)

[//]: # (The element ID for this nested tabbed card, used for everything that makes nesting cards and having tabs work)
[//]: # (We prepend the ID of the parent card tab - i.e. this file - to this element ID to ensure it is unique)
{% capture use_examples_htmlid %}{{ page.htmlid }}-useExamples{% endcapture %}

{% capture apacheClientContent %}
In each command below, substitute `$KROXYLICIOUS_BOOTSTRAP` for the bootstrap address of your proxy instance.

### i. Create a topic via the proxy
Create a topic called "my_topic" using the `kafka-topics.sh` command line client:
```shell
bin/kafka-topics.sh --create --topic my_topic --bootstrap-server $KROXYLICIOUS_BOOTSTRAP
```

### ii. Produce a string to your topic via the proxy
Produce the string "hello world" to your new topic using the `kafka-console-producer.sh` command line client:
```shell
echo "hello world" | bin/kafka-console-producer.sh --topic my_topic --bootstrap-server $KROXYLICIOUS_BOOTSTRAP
```

### iii. Consume your string from your topic via the proxy
Consume your string ("hello world") from your topic ("my_topic") using the `kafka-console-consumer.sh` command line client:
```shell
bin/kafka-console-consumer.sh --topic my_topic --from-beginning --bootstrap-server $KROXYLICIOUS_BOOTSTRAP
```
{% endcapture %}

{% capture kafClientContent %}
In each command below, substitute `$KROXYLICIOUS_BOOTSTRAP` for the bootstrap address of your proxy instance.

### i. Create a topic with Kaf via the proxy
Create a topic called "my_topic" using Kaf:
```shell
kaf -b $KROXYLICIOUS_BOOTSTRAP topic create my_topic
```

### ii. Produce a string to your topic with Kaf via the proxy
Produce the string "hello world" to your new topic:
```shell
echo "hello world" | kaf -b $KROXYLICIOUS_BOOTSTRAP produce my_topic
```

### iii. Consume your string from your topic with Kaf via the proxy
Consume your string ("hello world") from your topic ("my_topic"):
```shell
kaf -b $KROXYLICIOUS_BOOTSTRAP consume my_topic
```
{% endcapture %}

[//]: # (These IDs need to be identical across the tab elements and the tab content elements, so we declare them here to avoid confusion)
[//]: # (We prepend the ID of the parent element to these IDs to ensure they are unique)
{% capture apache_htmlid %}{{use_examples_htmlid}}-apacheClient{% endcapture %}
{% capture kaf_htmlid %}{{use_examples_htmlid}}-kafClient{% endcapture %}

{% capture use_example_tabs %}
{% include nested-card-tabbed/tab.html htmlid=apache_htmlid is_active_tab=true tab_title="Using Kroxylicious with the Apache Kafka® command line clients" %}
{% include nested-card-tabbed/tab.html htmlid=kaf_htmlid is_active_tab=false tab_title="Using Kroxylicious with the [Kaf](https://github.com/birdayz/kaf) command line client" %}
{% endcapture %}

{% capture use_example_content %}
{% include nested-card-tabbed/tab-content.html htmlid=apache_htmlid tabindex=0 is_active_tab=true content=apacheClientContent %}
{% include nested-card-tabbed/tab-content.html htmlid=kaf_htmlid tabindex=1 is_active_tab=false content=kafClientContent %}
{% endcapture %}

{% include nested-card-tabbed/card.html htmlid=use_examples_htmlid tabs=use_example_tabs content=use_example_content %}

[//]: # (====================================================================)
[//]: # (END - Use Section Examples Tabbed Card)
[//]: # (====================================================================)
