#! /bin/bash
export GO111MODULE=on
go mod verify
go mod tidy
go mod vendor