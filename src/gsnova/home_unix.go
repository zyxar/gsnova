// +build darwin freebsd netbsd openbsd linux

package main

import (
	"log"
	"os/user"
	"path/filepath"
	"runtime"

	"github.com/zyxar/gsnova/src/common"
)

func init() {
	if runtime.GOARCH == "arm" {
		common.Home = "/sdcard/gsnova"
	} else {
		if usr, err := user.Current(); err != nil {
			log.Fatalln(err)
		} else {
			common.Home = filepath.Join(usr.HomeDir, ".gsnova")
		}
	}
}
