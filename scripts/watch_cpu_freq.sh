#!/bin/bash
watch -n1 "cat /proc/cpuinfo | grep MHz | awk 'BEGIN{min=61; max=75}{if (NR >= min) {if (NR <= max) { {print $4} } } }'";