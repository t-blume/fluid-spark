#!/bin/bash
# convenience wrapper: $1:=config, $2:=resume point (first snapshot), $3:=end point (last snapshot)
sbt "runMain Main $1 $2 $3"
