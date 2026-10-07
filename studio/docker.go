// Docker Engine access: the health check's version, and the node containers
// that deployed flows run in. Every node container carries the labels
// studio.flow and studio.node (and studio.instance for one of a consumer's
// instances); nothing else about it is stored.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const (
	labelFlow     = "studio.flow"
	labelNode     = "studio.node"
	labelInstance = "studio.instance" // only on a consumer's instances 1..n
)

// newDocker builds a client from DOCKER_HOST etc.; the default is the socket
// at /var/run/docker.sock, which compose mounts into the studio container.
func newDocker() (*client.Client, error) {
	return client.New(client.FromEnv)
}

// dockerVersion returns the engine version; the client negotiates the API
// version on its first request.
func dockerVersion(ctx context.Context, cli *client.Client) (string, error) {
	v, err := cli.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return "", err
	}
	return v.Version, nil
}

// self is what node containers copy from the control plane's own container.
type self struct{ image, network string }

// discoverSelf inspects the control plane's own container (its hostname is the
// container id) for its image and compose network. STUDIO_IMAGE and
// STUDIO_NETWORK override both, for a control plane run outside compose.
func discoverSelf(ctx context.Context, cli *client.Client) (self, error) {
	s := self{image: os.Getenv("STUDIO_IMAGE"), network: os.Getenv("STUDIO_NETWORK")}
	if s.image != "" && s.network != "" {
		return s, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return s, err
	}
	res, err := cli.ContainerInspect(ctx, host, client.ContainerInspectOptions{})
	if err != nil {
		return s, fmt.Errorf("inspect own container %s (outside compose set STUDIO_IMAGE and STUDIO_NETWORK): %w", host, err)
	}
	c := res.Container
	if s.image == "" && c.Config != nil {
		// The tag, not the image id: `compose up` rebuilds the studio image without
		// recreating a running container, and the id this container was created
		// from is then gone from the image store ("No such image" on deploy).
		s.image = c.Config.Image
	}
	if s.network == "" && c.NetworkSettings != nil {
		for name := range c.NetworkSettings.Networks {
			s.network = name // ponytail: compose gives the studio one network; the first wins if there are more
			break
		}
	}
	if s.image == "" || s.network == "" {
		return s, fmt.Errorf("own container %s: no image or network", host)
	}
	return s, nil
}

// startNode creates and starts one node container on the control plane's network.
func startNode(ctx context.Context, cli *client.Client, me self, brokers string, spec NodeSpec) error {
	env, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	name := containerName(spec.Flow, spec.Node, spec.Instance)
	labels := map[string]string{labelFlow: spec.Flow, labelNode: spec.Node}
	if spec.Instance > 0 {
		labels[labelInstance] = strconv.Itoa(spec.Instance)
	}
	res, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:  me.image,
			Cmd:    []string{"node"},
			Env:    []string{"STUDIO_NODE=" + string(env), "KAFKA_BROKERS=" + brokers},
			Labels: labels,
		},
		HostConfig: &container.HostConfig{NetworkMode: container.NetworkMode(me.network)},
		NetworkingConfig: &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{me.network: {}},
		},
	})
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if _, err := cli.ContainerStart(ctx, res.ID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	return nil
}

// flowContainers lists the node containers of one flow, or of every flow when
// flow is "", in any state.
func flowContainers(ctx context.Context, cli *client.Client, flow string) ([]container.Summary, error) {
	label := labelFlow
	if flow != "" {
		label += "=" + flow
	}
	res, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: make(client.Filters).Add("label", label)})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// removeContainers stops each running container (SIGTERM, 5 s grace so a
// consumer commits and leaves its group) and removes it. It tries every
// container and returns the first error other than not-found.
func removeContainers(ctx context.Context, cli *client.Client, cs []container.Summary) error {
	grace := 5
	var first error
	keep := func(err error) {
		if err != nil && !cerrdefs.IsNotFound(err) && first == nil { // a container removed by hand is already gone
			first = err
		}
	}
	for _, c := range cs { // ponytail: one at a time; parallel if flows grow past a handful of nodes
		if c.State == container.StateRunning {
			_, err := cli.ContainerStop(ctx, c.ID, client.ContainerStopOptions{Timeout: &grace})
			keep(err)
		}
		_, err := cli.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true})
		keep(err)
	}
	return first
}
