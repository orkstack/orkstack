package main

import "github.com/spf13/cobra"

// newTaskCommand is `orkctl task`, the sibling local tool of `orkctl
// activity`: where `activity test` checks an image against the contract,
// `task run` runs one task of one orkflow on this machine.
func newTaskCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Run one task of an orkflow on this machine",
	}
	cmd.AddCommand(newTaskRunCommand(g))
	return cmd
}
