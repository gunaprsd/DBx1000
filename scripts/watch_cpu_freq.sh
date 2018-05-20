#!/bin/bash
watch -n1 "cat /proc/cpuinfo | grep MHz | awk 'BEGIN{min=0; max=10}{if (NR >= min) {if (NR <= max) { {print $4} } } }'";
