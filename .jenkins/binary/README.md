# Binary Build

Builds the Hadoop tar file within a Docker image.

The `build.sh` is invoked by Jenkins.
Jobs can be found [here](https://ci.uberinternal.com/view/Hadoop%20Automation/).

## Building Locally

Although the primary purpose is building through Jenkins CI.

Binary build has limited support for building locally on Macs.
Local builds require `Docker version 19.03.5` or later.

```
mkdir -p /Users/$(whoami)/hadoop-build
ln -s ${location_of_hadoop_common} /Users/$(whoami)/hadoop-build/hadoop
# Eg:  ln -s /Users/mmathew/UberProjects/java/hadoop-common /Users/$(whoami)/hadoop-build/hadoop
ls -al /Users/$(whoami)/hadoop-build/hadoop
export WORKSPACE=/Users/$(whoami)/hadoop-build
export ARTIFACTORY_AUTH=something:something
export ARTIFACTS_DIR_NAME=artifacts
export BRANCH=master-int-2.8.2  # Use the required branch name or the commit hash.
export BUILD_NUMBER=21

# Start the build.
/Users/$(whoami)/hadoop-build/hadoop/.jenkins/binary/build.sh
```

The artifactory auth would fail and the build would break on uploading to artifactory.
The `hadoop.tar` file can be found at `/Users/$(whoami)/hadoop-build/artifacts`
