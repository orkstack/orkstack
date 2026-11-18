//go:build !linux

package orker

import "errors"

func askDriver(string, bool) (string, error) {
	return "", errors.New("a gpu unit probe needs a Linux host")
}
