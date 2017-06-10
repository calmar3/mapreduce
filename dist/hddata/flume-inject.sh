#!/usr/bin/env bash
sudo /flume/bin/flume-ng agent --conf /flume/conf --conf-file /flume/conf/flume-conf.properties --name agent1 -Dflume.root.logger=INFO,console