#!/usr/bin/env bash

docker run -it --name airblow --rm \
    --mount type=bind,src="$HOME"/Projects/robot-dreams-dataeng/lesson7/airflow/workdir,dst=/opt/airflow \
    airblow
