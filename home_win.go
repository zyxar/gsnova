// +build windows

package main

import (
	"os"
	"path/filepath"

	"github.com/zyxar/gsnova/common"
)

func init() {
	common.Home = filepath.Dir(os.Args[0])
}
