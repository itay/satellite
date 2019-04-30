/*
Copyright 2019 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/* Check POC
Time skew:
each node should send his server time via `serf` only to the master nodes
(in order to avoid wasting too much traffic) which will be used as a reference.
If multiple master nodes are present they will compare data and alert in case
the stored values exceeds a certain threshold.
This check will be `time.go` and will implement `TimeSkewChecker`

Every coordinator node (one of kubernetes masters) performs an instance
of this algorithm.

For each of remaining cluster nodes (including other coordinator nodes):
* Selected coordinator node records it’s local timestamp (in UTC). Let’s call
  this timestamp T1Start.
* Coordinator initiates a “ping” grpc request to the node. Can be with empty
  payload.
* The node responds to the ping request replying with node’s local timestamp
  (in UTC) in the payload. Let’s call this timestamp T2.
* As the coordinator received the response, coordinator gets second local
  timestamp. Let’s call it T1End.
* Coordinator calculates the latency between itself and the node:
  (T1End-T1Start)/2. Let’s call this value Latency.
* Coordinator calculates the time skew between itself and the node:
  T2-T1Start-Latency. Let’s call this value Skew. Can be negative which would
  mean the node time is falling behind.
* Now you can add Latency and abs(Skew) to the node’s histograms.
*/

package monitoring

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gravitational/satellite/agent"
	"github.com/gravitational/satellite/agent/health"
	pb "github.com/gravitational/satellite/agent/proto/agentpb"
	"github.com/gravitational/trace"

	serf "github.com/hashicorp/serf/client"
	log "github.com/sirupsen/logrus"
)

const (
	// timeSkewCheckerID specifies the check name
	timeSkewCheckerID = "time-skew-checker"
	// timeSkewThreshold set the default threshold of the acceptable time
	// difference between nodes
	timeSkewThreshold = 1.0 * time.Millisecond
)

// timeSkewChecker is a checker that verifies that the time difference between
// cluster nodes remains withing the specified threshold
type timeSkewChecker struct {
	self           serf.Member
	serfClient     agent.SerfClient
	serfRPCAddr    string
	serfMemberName string
	mux            sync.Mutex
	logger         log.Entry
}

// TimeSkewCheckerConfig is used to store all the configuration related to the current check
type TimeSkewCheckerConfig struct {
	// SerfRPCAddr is the address used by the Serf RPC client to communicate
	SerfRPCAddr string
	// SerfMemberName is the name associated to this node in Serf
	SerfMemberName string
	// NewSerfClient is an optional Serf Client function that can be used instead
	// of the default one. If not specified it will fallback to the default one
	NewSerfClient agent.NewSerfClientFunc
	// TODO
	SatellitePort int32
	// TODO
	SatelliteCAFile string
	// TODO
	SatelliteCertFile string
	// TODO
	SatelliteKeyFile string
	// NewClient is an optional Satellite Client function that can be used instead
	// of the default one. If not specified it will create a client to Satellite servers
	NewSatelliteClient agent.NewClientFunc
}

// CheckAndSetDefaults is an helper function which just check that the provided
// check config is in order and eventually set default values where needed/possible
func (c *TimeSkewCheckerConfig) CheckAndSetDefaults() error {
	if c.SerfRPCAddr == "" {
		return trace.BadParameter("serf rpc address can't be empty")
	}
	if c.SerfMemberName == "" {
		return trace.BadParameter("serf member name can't be empty")
	}
	if c.NewSerfClient == nil {
		c.NewSerfClient = agent.NewSerfClient
	}
	if c.NewSatelliteClient == nil {
		c.NewSatelliteClient = agent.NewClient
	}
	return nil
}

// NewTimeSkewChecker returns a checker that verifies time skew of nodes in
// the cluster
func NewTimeSkewChecker(conf TimeSkewCheckerConfig) (c health.Checker, err error) {
	err = conf.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	logger := log.WithFields(log.Fields{trace.Component: "timeSkew"})
	logger.Debugf("using Serf IP: %v", conf.SerfRPCAddr)
	logger.Debugf("using Serf Name: %v", conf.SerfMemberName)

	client, err := conf.NewSerfClient(serf.Config{
		Addr: conf.SerfRPCAddr,
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}

	// retrieve other nodes using Serf members
	nodes, err := client.Members()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	// finding what is the current node
	var self serf.Member
	for _, node := range nodes {
		logger.Debugf("node %s status %s", node.Name, node.Status)
		if node.Status != pb.MemberStatus_Alive.String() {
			continue
		}
		if node.Name == conf.SerfMemberName {
			self = node
			break // self node found, breaking out of the for loop
		}
	}
	if self.Name == "" {
		return nil, trace.NotFound("failed to find Serf member with name %s", conf.SerfMemberName)
	}

	return &timeSkewChecker{
		self:           self,
		serfClient:     client,
		serfRPCAddr:    conf.SerfRPCAddr,
		serfMemberName: conf.SerfMemberName,
		logger:         *logger,
	}, nil
}

// Name returns the checker name
// Implements health.Checker
func (c *timeSkewChecker) Name() string {
	return timeSkewCheckerID
}

// Check checks the values returned by the check function and set health probes
// according to the returned values and error
// Implements health.Checker
func (c *timeSkewChecker) Check(ctx context.Context, r health.Reporter) {
	probeSeverity, err := c.check(ctx, r)

	if err != nil {
		c.logger.Error(err.Error())
		r.Add(NewProbeFromErr(c.Name(), "", err))
		return
	}
	r.Add(&pb.Probe{
		Checker:  c.Name(),
		Status:   pb.Probe_Running,
		Severity: probeSeverity,
	})
}

// check verifies the time skew between master nodes is lower than the desired
// threshold
func (c *timeSkewChecker) check(ctx context.Context, r health.Reporter) (probeSeverity pb.Probe_Severity, err error) {

	/*
		Fetch Serf cluster members and start iterating and running the check
	*/
	client := c.serfClient

	nodes, err := client.Members()
	if err != nil {
		return pb.Probe_None, trace.Wrap(err)
	}

	probeSeverity, err = c.checkTimeSkew(ctx, nodes, client)
	if err != nil {
		return pb.Probe_None, trace.Wrap(err)
	}

	return probeSeverity, nil
}

func (c *timeSkewChecker) checkTimeSkew(ctx context.Context, nodes []serf.Member, client agent.SerfClient) (probeSeverity pb.Probe_Severity, err error) {
	probeSeverity = pb.Probe_None

	for _, node := range nodes {
		// skipping nodes that are not alive (failed, removed, etc..)
		if strings.ToLower(node.Status) != strings.ToLower(pb.MemberStatus_Alive.String()) {
			c.logger.Debugf("skipping node %s because status is %q", node.Name, node.Status)
			continue
		}
		// skip pinging self
		if c.self.Addr.String() == node.Addr.String() {
			c.logger.Debugf("skipping analyzing self node (%s)", node.Name)
			continue
		}
		c.logger.Debugf("node %s status %s", node.Name, node.Status)

		client, err := agent.NewClient(
			fmt.Sprintf("%s:%s", node.Addr.String(), node.agentPort), // TODO store agent conf in Serf?
			caFile, certFile, keyFile)

		skew, err := c.getTimeSkew(ctx, client, node)
		if err != nil {
			return probeSeverity, err
		}

		if skew >= timeSkewThreshold {
			c.logger.Debugf("$s <-> %s => time skew (%s) over threshold %s",
				c.self.Name, node.Name, skew, timeSkewThreshold)
			probeSeverity = pb.Probe_Warning
		}

		c.logger.Debugf("$s <-> %s => time skew (%s) within threshold %s",
			c.self.Name, node.Name, skew, timeSkewThreshold)
	}

	return probeSeverity, nil
}

func (c *timeSkewChecker) getTimeSkew(ctx context.Context, client agent.Client, node serf.Member) (skew time.Duration, err error) {
	/*
		* Selected coordinator node records it’s local timestamp (in UTC). Let’s call
		  this timestamp T1Start.
	*/

	t1Start := time.Now().UTC()
	c.logger.Debugf("%s check start UTC time %s", c.self.Name, t1Start.String())

	/*
		* Coordinator initiates a “ping” gRPC request to the node. Can be with empty
		  payload.
		* The node responds to the ping request replying with node’s local timestamp
		  (in UTC) in the payload. Let’s call this timestamp T2.
	*/
	t2 := time.Now().UTC() // TODO - target node's time, retrieved via gRPC
	c.logger.Debugf("%s retrieved UTC time %s", node.Name, t2)

	/*
		* Coordinator calculates the latency between itself and the node:
		  (T1End-T1Start)/2. Let’s call this value Latency.
	*/
	nodesLatency := time.Since(t1Start) / 2
	c.logger.Debugf("%s node timeSkew check latency is %s", node.Name, nodesLatency)

	/*
		* Coordinator calculates the time skew between itself and the node:
		  T2-T1Start-Latency. Let’s call this value Skew. Can be negative which would
		  mean the node time is falling behind.
	*/
	skew = t2.Sub(t1Start) - nodesLatency
	c.logger.Debugf("$s <-> %s => time skew is %s", c.self.Name, node.Name, skew.String())

	/*
	* Now you can add Latency and abs(Skew) to the node’s histograms.
	 */
	// TODO

	return skew, nil
}
