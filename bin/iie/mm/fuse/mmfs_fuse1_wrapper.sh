#!/bin/bash

ENVS=""
NEW_ARGS=""
oargs=0
iter=0

for i in $@; do
    iter+=1
    if [ $iter -eq 1 ]; then
        >&2 echo "Mounting: ignore DEVICE $i"
        continue
    fi
    if [ "x$i" == 'x-o' ]; then
        oargs=1
        continue
    fi
    if [ "x$oargs" == "x1" ]; then
        OARGS=`echo $i | sed 's/,/ /g'`
        for j in $OARGS; do
            if [[ $j == *"ps="* ]]; then
                ENVS+="$j "
            elif [[ $j == *"namespace="* ]]; then
                ENVS+="$j "
            elif [[ $j == *"debug="* ]]; then
                ENVS+="$j "
            elif [[ $j == *"uris="* ]]; then
                ENVS+="$j "
            elif [[ $j == *"noatime="* ]]; then
                ENVS+="$j "
            else
                NEW_ARGS+="-o $j "
            fi
        done
        continue
    fi
    NEW_ARGS+="$i "
done

export MMFS_HOME=/home/macan/workspace/dservice
export LD_LIBRARY_PATH=$MMFS_HOME/build:$MMFS_HOME/lib:$LD_LIBRARY

bash -c "$ENVS $MMFS_HOME/build/mmfs_v1 $NEW_ARGS -d -f > /tmp/mmfs.log 2>&1 &"
