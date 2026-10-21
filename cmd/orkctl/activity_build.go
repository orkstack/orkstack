package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

// `orkctl activity build <path|git-url> --tag <ref> [--push]` (§4.6).
//
// It is an explicit, out-of-band step, and it prints the pinned reference to
// paste into `uses:`. That is the whole product: turning a Dockerfile into a
// string an Orkflow can name immutably.
//
// EVERY BUILD PATH PUSHES, and that is not a policy preference. A locally
// built, never-pushed image has NO registry manifest digest — `--iidfile` and
// `--quiet` give a local config ID, which is not a manifest digest and does
// not transfer to a second Orker. So an unpushed image cannot be named by
// `uses:` in any deployment with more than one Orker, and pretending otherwise
// would produce an Orkflow that runs on the machine that built it and nowhere
// else. `--push` therefore defaults to on, and `--push=false` is available for a
// local smoke test whose result is explicitly not usable as a pin.
func newActivityBuildCommand(g *globalFlags) *cobra.Command {
	var (
		tag      string
		push     bool
		platform string
		file     string
	)
	cmd := &cobra.Command{
		Use:   "build <path|git-url>",
		Short: "Build an activity image and print the reference to pin",
		Long: "Build an OCI image from a Dockerfile context and push it, printing the\n" +
			"pinned reference to paste into `uses:`.\n\n" +
			"The push is what mints a real registry manifest digest. A locally built,\n" +
			"never-pushed image has none — a local image ID is not a manifest digest and\n" +
			"does not transfer to a second Orker — so `--push=false` produces an image you\n" +
			"can smoke-test but cannot pin.\n\n" +
			"This wraps the ordinary container toolchain and adds no privileged path: a\n" +
			"first-party image is built exactly the way any third-party image is.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(tag) == "" {
				return fmt.Errorf("--tag is required: it names the reference this image is pushed to, " +
					"e.g. --tag ghcr.io/acme/migrate:v3")
			}
			context := args[0]
			out := cmd.OutOrStdout()

			build := []string{"build", "--tag", tag}
			if platform != "" {
				build = append(build, "--platform", platform)
			}
			if file != "" {
				build = append(build, "--file", file)
			}
			build = append(build, context)
			if err := runDocker(cmd, out, build...); err != nil {
				return err
			}
			if !push {
				fmt.Fprintf(out, "\nBuilt %s locally.\n", tag)
				fmt.Fprintln(out, "NOT pushed, so it has no registry manifest digest and cannot be "+
					"pinned by `uses:`. Re-run without --push=false to publish it.")
				return nil
			}
			if err := runDocker(cmd, out, "push", tag); err != nil {
				return err
			}
			digest, err := manifestDigest(cmd, tag)
			if err != nil {
				// The push succeeded; only the convenience readback failed.
				fmt.Fprintf(out, "\nPushed %s.\n", tag)
				fmt.Fprintf(out, "Could not read back its index digest (%v); "+
					"`uses: %s` still resolves at apply time.\n", err, tag)
				return nil
			}
			reference := tag
			if at := strings.LastIndex(tag, ":"); at > strings.LastIndex(tag, "/") {
				reference = tag[:at]
			}
			fmt.Fprintf(out, "\nPushed %s\n\n", tag)
			fmt.Fprintln(out, "Pin it in an Orkflow with either spelling:")
			fmt.Fprintf(out, "  uses: %s\n", tag)
			fmt.Fprintf(out, "  uses: %s@%s\n", reference, digest)
			fmt.Fprintln(out, "\nThe first resolves the tag to that digest at apply time and records it "+
				"in the version document; the second is already pinned. Either way the run "+
				"executes the digest, never the tag.")
			return nil
		},
	}
	cmd.Flags().StringVar(&tag, "tag", "", "the registry reference to build and push (required)")
	cmd.Flags().BoolVar(&push, "push", true, "push after building; --push=false builds only")
	cmd.Flags().StringVar(&platform, "platform", "", "target platform(s), e.g. linux/amd64,linux/arm64")
	cmd.Flags().StringVar(&file, "file", "", "path to the Dockerfile")
	return cmd
}

// runDocker executes the container toolchain, streaming its output through.
//
// The output is streamed rather than captured because a build is long and its
// progress IS the feedback; swallowing it and printing a summary at the end
// would make a two-minute build look like a hang.
func runDocker(cmd *cobra.Command, out interface{ Write([]byte) (int, error) }, args ...string) error {
	binary, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("`docker` is not on PATH: `orkctl activity build` wraps the ordinary " +
			"container toolchain rather than reimplementing it")
	}
	proc := exec.CommandContext(cmd.Context(), binary, args...)
	proc.Stdout = out
	proc.Stderr = os.Stderr
	if err := proc.Run(); err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// manifestDigest reads back the index digest the registry assigned.
func manifestDigest(cmd *cobra.Command, tag string) (string, error) {
	binary, err := exec.LookPath("docker")
	if err != nil {
		return "", err
	}
	proc := exec.CommandContext(cmd.Context(), binary,
		"buildx", "imagetools", "inspect", tag, "--format", "{{.Manifest.Digest}}")
	output, err := proc.Output()
	if err != nil {
		return "", fmt.Errorf("read back the pushed digest: %w", err)
	}
	digest := strings.TrimSpace(string(output))
	if digest == "" {
		return "", fmt.Errorf("the toolchain reported no digest for %s", tag)
	}
	return digest, nil
}
