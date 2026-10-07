// Docker Engine access. M1 only proves the socket works; M2 adds the
// container calls that run nodes.
package main

import (
	"context"

	"github.com/moby/moby/client"
)

// newDocker builds a client from DOCKER_HOST etc.; the default is the socket
// at /var/run/docker.sock, which compose mounts into the studio container.
func newDocker() (*client.Client, error) {
	return client.New(client.FromEnv)
}

// dockerVersion negotiates the API version and returns the engine version.
func dockerVersion(ctx context.Context, cli *client.Client) (string, error) {
	if _, err := cli.Ping(ctx, client.PingOptions{NegotiateAPIVersion: true}); err != nil {
		return "", err
	}
	v, err := cli.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return "", err
	}
	return v.Version, nil
}
