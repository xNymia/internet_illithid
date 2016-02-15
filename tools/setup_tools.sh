#!/bin/bash
wget https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.0/elasticsearch-2.2.0.tar.gz
wget https://download.elastic.co/kibana/kibana/kibana-4.4.1-linux-x64.tar.gz
tar xzf elasticsearch-2.2.0.tar.gz
tar xzf kibana-4.4.1-linux-x64.tar.gz
rm elasticsearch-2.2.0.tar.gz
rm kibana-4.4.1-linux-x64.tar.gz