#!/usr/bin/env bash
echo "export PIG_HOME = /pig" >> ~/.bashrc
echo "PATH  = $PATH:/pig/bin" >> ~/.bashrc
echo "PIG_CLASSPATH = $HADOOP_HOME/conf" >> ~/.bashrc