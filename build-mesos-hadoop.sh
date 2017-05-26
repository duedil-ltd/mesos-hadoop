#!/bin/bash
#
# This script can be used whenever you need to build an executor package
# for mesos-hadoop.
#
# Once you have made changes to the code, running
#
#   ./build-mesos-hadoop.sh -h {Hadoop version} -c {CDH version}
#
# will:
#
#   * Execute mvn clean package
#   * Download hadoop-<Haddop versions>-cdh<CDH version>.tar.gz
#   * Extract the package's contents
#   * Copy the artefact resulting from the Maven build to the
#     right place
#   * Create all the necessary symlinks
#   * Create a tar file ready to be uploaded to HDFS
#   * Place a copy of the mesos-hadoop artefact in the current directory
#   * Remove the work directory
#
# After this, you need to upload the executor package to HDFS and the
# mesos-hadoop jar to the Hadoop lib dir on the job trackers
# (optional: remove the CDH tar.gz file).
#
# Example usage:
#
#   ./build-mesos-hadoop.sh -h 2.6.0 -c 5.9.0
#
# Alternatively you can use --hadoop and --cdh in place of -h and -c,
# respectively.
#

set -e
export PATH=/bin:/usr/bin:/usr/local/bin

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -h|--hadoop)
            HADOOP="$2"
            shift
            ;;
        -c|--cdh)
            CDH=$2
            shift
            ;;
        --help)
            head -25 $0 | tail -24
            exit 0
            ;;
        *)
            head -25 $0 | tail -24
            exit 1
            ;;
    esac
    shift
done

mvn clean package

CDH_PACKAGE="hadoop-${HADOOP}-cdh${CDH}.tar.gz"
CDH_DIR="${CDH_PACKAGE/.tar.gz/}"
if [ ! -r "${CDH_PACKAGE}" ]; then
    echo "Downloading CDH ${CDH} with Hadoop ${HADOOP}..."
    rm -f "${CDH_PACKAGE}"
    curl -L -O http://archive.cloudera.com/cdh5/cdh/5/${CDH_PACKAGE}
fi

echo "Unpacking CDH..."
tar xf "${CDH_PACKAGE}"

pushd "${CDH_DIR}"
echo "Copying artefact..."
cp ../target/mesos-hadoop-*.jar share/hadoop/common/lib

echo "Creating symlinks..."
mv bin bin-mapreduce2
mv examples examples-mapreduce2
ln -s bin-mapreduce1 bin
ln -s examples-mapreduce1 examples

pushd etc
mv hadoop hadoop-mapreduce2
ln -s hadoop-mapreduce1 hadoop
popd

pushd share/hadoop
rm -f mapreduce
ln -s mapreduce1 mapreduce
popd
popd

echo "Creating executor tar file..."
VERSION=`grep -m 1 '<version>' pom.xml | sed -e 's/<[^>]*>//g' | tr -d ' '`
PACKAGE="${CDH_DIR}-mesos-hadoop-${VERSION}.tar.gz"
tar zcf "${PACKAGE}" "${CDH_DIR}"

echo "Copying mesos-hadoop artefact to the current directory..."
cp target/mesos-hadoop-*.jar .

echo "Removing work directory..."
rm -rf ${CDH_DIR}

echo "Your file is ${PACKAGE}"
echo "Job's done!"
