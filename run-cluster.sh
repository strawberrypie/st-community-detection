#!/bin/bash

sbt assembly
spark-submit --master yarn-cluster ./target/scala-2.10/st-community-detection.jar