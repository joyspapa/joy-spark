#!/bin/sh
if [ $# -ne 1 ]; then
    echo "Usage: $0 [date, ex)2018-08-10]"
    exit 2
fi

DATE=$1
echo "starting logDetail batch for $DATE..."
#cp /usr/local/spark/conf/ext.log4j.properties.template /usr/local/spark/conf/log4j.properties
#sed -i "s/%date%/$DATE/g" /usr/local/spark/conf/log4j.properties

/opt/spark/bin/spark-submit \
                --name "logDetail batch ($DATE)" \
                --class com.obzen.spark.application.ObzLogDetailBatchAppl \
                --master yarn --deploy-mode cluster \
                --files /usr/local/spark/conf/log4j.properties \
                --driver-memory 3g --executor-memory 5g --executor-cores 1 --num-executors 3 \
                --conf "spark.memory.fraction=0.3" --conf "spark.memory.storageFraction=0.4" \
                --conf "spark.yarn.executor.memoryOverhead=3072" \
                /usr/local/spark/lib/obzen-spark-0.9.7-SNAPSHOT-all.jar \
                --batchDate $DATE \
                --inDataPath  "/user/ecube/log-planet/ECRM_LOG-DE1533286081-ENT7090-HDFS/_timestamp=$DATE" \
                --outDataPath  "/user/ecube/log-planet/batch/out/$DATE/LogDetail"

echo "logDetail batch for $DATE finished"