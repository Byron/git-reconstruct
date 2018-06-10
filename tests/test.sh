#!/bin/bash

exe=${1:?Need executable}

echo XXXXXXXXXXXXXXXXXXX | "$exe" $PWD
