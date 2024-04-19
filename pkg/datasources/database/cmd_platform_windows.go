//go:build windows
// +build windows

package database

import (
	"syscall"

	"os/exec"
)

func setCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
