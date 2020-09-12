#!/bin/sh

DIR=$1
mkdir -p ${DIR}/logicFiles/
mkdir -p ${DIR}/storageFiles/

rm -f ${DIR}/logicFiles/*
rm -f ${DIR}/storageFiles/*
rm -f ${DIR}/manifest
rm -f ${DIR}/kvstore