// Command orkctl is the OrkStack CLI. It compiles and applies orkflows,
// submits and observes runs, and inspects the activities the ork engine can
// run, talking to the engine's Connect API over unencrypted HTTP/2 (h2c).
package main

import "os"

func main() {
	os.Exit(Execute(os.Args[1:], os.Stdout, os.Stderr))
}
