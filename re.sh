#!/bin/zsh
rm -rf /Users/every/docker/gf/flow/EMP/send/BDRA/
#mvn clean install
#rm -f /Users/every/docker/gf/extensions/nifi-writefile-nar-1.0.nar
#cp nifi-writefile-nar/target/nifi-writefile-nar-1.0.nar /Users/every/docker/gf/extensions/
docker rm -f nifigf
docker run --name nifigf --cpus=4 -m=5G -v /Users/every/docker/gf/extensions:/opt/nifi/nifi-current/extensions -v /Users/every/docker/gf/conf:/opt/nifi/nifi-current/conf  -v /Users/every/docker/gf/flow:/opt/nifi/nifi-current/flowfiles -p 9080-9090:8080-8090 -d apache/nifi:1.12.1
#docker run --name nifigf --cpus=4 -m=8G -v /Users/every/docker/gf/flow:/opt/nifi/nifi-current/flowfiles -p 9080-9090:8080-8090 -d apache/nifi:1.12.1
docker logs -f nifigf


#http://geocoding:6020/gc?address=${org_address}
