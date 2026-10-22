// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package clustermanager

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	cache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/allocator/id"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	ipgenerator "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/generator/ip"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

const (
	// Maximum node IDs that can be generated. Limited to maximum nodes supported by k8s.
	maxNodeIDs = 5000
)

// nodeAllocationController allocates node IDs and transit switch port addresses.
type nodeAllocationController struct {
	kube         kube.Interface
	watchFactory *factory.WatchFactory
	stopChan     chan struct{}
	stopOnce     sync.Once
	wg           *sync.WaitGroup

	nodeQueue   workqueue.TypedRateLimitingInterface[string]
	nodeHandler cache.ResourceEventHandlerRegistration

	// ID allocator for the nodes
	nodeIDAllocator id.Allocator

	// Transit switch IP generators allocate node transit switch port addresses.
	transitSwitchIPv4Generator *ipgenerator.IPGenerator
	transitSwitchIPv6Generator *ipgenerator.IPGenerator
}

func newNodeAllocationController(ovnClient *util.OVNClusterManagerClientset, wf *factory.WatchFactory) (*nodeAllocationController, error) {
	// Since we don't assign 0 to any node, create IDAllocator with one extra element in maxIds.
	nodeIDAllocator := id.NewIDAllocator("NodeIDs", maxNodeIDs+1)
	// Reserve the id 0. We don't want to assign this id to any of the nodes.
	if err := nodeIDAllocator.ReserveID("zero", 0); err != nil {
		return nil, fmt.Errorf("idAllocator failed to reserve id 0")
	}
	if err := nodeIDAllocator.ReserveID("one", 1); err != nil {
		return nil, fmt.Errorf("idAllocator failed to reserve id 1")
	}

	kube := &kube.Kube{
		KClient: ovnClient.KubeClient,
	}
	wg := &sync.WaitGroup{}

	var transitSwitchIPv4Generator, transitSwitchIPv6Generator *ipgenerator.IPGenerator
	var err error
	if config.IPv4Mode {
		transitSwitchIPv4Generator, err = ipgenerator.NewIPGenerator(config.ClusterManager.V4TransitSubnet)
		if err != nil {
			return nil, fmt.Errorf("error creating IP Generator for v4 transit subnet %s: %w", config.ClusterManager.V4TransitSubnet, err)
		}
	}

	if config.IPv6Mode {
		transitSwitchIPv6Generator, err = ipgenerator.NewIPGenerator(config.ClusterManager.V6TransitSubnet)
		if err != nil {
			return nil, fmt.Errorf("error creating IP Generator for v6 transit subnet %s: %w", config.ClusterManager.V6TransitSubnet, err)
		}
	}

	nac := &nodeAllocationController{
		kube:                       kube,
		watchFactory:               wf,
		stopChan:                   make(chan struct{}),
		wg:                         wg,
		nodeIDAllocator:            nodeIDAllocator,
		transitSwitchIPv4Generator: transitSwitchIPv4Generator,
		transitSwitchIPv6Generator: transitSwitchIPv6Generator,
	}

	if err := nac.initHandlers(); err != nil {
		return nil, err
	}
	return nac, nil
}

func (nac *nodeAllocationController) initHandlers() error {
	nac.nodeQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.DefaultRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "node-allocation"},
	)

	var err error
	nac.nodeHandler, err = nac.watchFactory.NodeInformer().AddEventHandler(
		factory.WithUpdateHandlingForObjReplace(cache.ResourceEventHandlerFuncs{
			AddFunc:    nac.onNodeAdd,
			UpdateFunc: nac.onNodeUpdate,
			DeleteFunc: nac.onNodeDelete,
		}),
	)
	if err != nil {
		nac.Stop()
		return fmt.Errorf("unable to add node event handler: %w", err)
	}
	return nil
}

func (nac *nodeAllocationController) onNodeAdd(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	nac.nodeQueue.Add(node.Name)
}

func (nac *nodeAllocationController) onNodeUpdate(oldObj, newObj interface{}) {
	oldNode, ok1 := oldObj.(*corev1.Node)
	newNode, ok2 := newObj.(*corev1.Node)
	if !ok1 || !ok2 {
		return
	}
	if !util.NodeIDAnnotationChanged(oldNode, newNode) &&
		!util.NodeTransitSwitchPortAddrAnnotationChanged(oldNode, newNode) &&
		util.NoHostSubnet(oldNode) == util.NoHostSubnet(newNode) &&
		!nac.nodeNeedsUpdate(newNode) {
		return
	}
	nac.nodeQueue.Add(newNode.Name)
}

func (nac *nodeAllocationController) onNodeDelete(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		node, ok = tombstone.Obj.(*corev1.Node)
		if !ok {
			return
		}
	}
	nac.nodeQueue.Add(node.Name)
}

func (nac *nodeAllocationController) runNodeWorker(_ context.Context) {
	for nac.processNodeItem() {
	}
}

func (nac *nodeAllocationController) processNodeItem() bool {
	nodeName, shutdown := nac.nodeQueue.Get()
	if shutdown {
		return false
	}
	defer nac.nodeQueue.Done(nodeName)

	err := nac.reconcileNode(nodeName)
	if err != nil {
		if nac.nodeQueue.NumRequeues(nodeName) < controller.DefaultMaxAttempts {
			klog.V(4).Infof("Error reconciling node %s: %v, requeuing", nodeName, err)
			nac.nodeQueue.AddRateLimited(nodeName)
			return true
		}
		klog.Errorf("Dropping node %s out of queue after %d attempts: %v", nodeName, controller.DefaultMaxAttempts, err)
	}
	nac.nodeQueue.Forget(nodeName)
	return true
}

func (nac *nodeAllocationController) nodeNeedsUpdate(node *corev1.Node) bool {
	if needsNodeAllocation(node) {
		return true
	}
	nodeID, _ := util.GetNodeID(node)
	return nodeID != nac.nodeIDAllocator.GetID(node.Name)
}

func (nac *nodeAllocationController) reconcileNode(nodeName string) error {
	node, err := nac.watchFactory.GetNode(nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nac.handleDeleteNode(nodeName)
		}
		return fmt.Errorf("failed to get node %s from informer: %w", nodeName, err)
	}

	if !nac.nodeNeedsUpdate(node) {
		return nil
	}
	return nac.handleAddUpdateNodeEvent(node)
}

func (nac *nodeAllocationController) WatchNodes(ctx context.Context, workers int) error {
	if nac.nodeQueue == nil {
		return fmt.Errorf("nodeQueue is not initialized")
	}

	for i := 0; i < workers; i++ {
		nac.wg.Add(1)
		go func() {
			defer nac.wg.Done()
			wait.UntilWithContext(ctx, nac.runNodeWorker, time.Second)
		}()
	}

	return nil
}

// Start starts the node allocation controller to watch the kubernetes nodes
func (nac *nodeAllocationController) Start(_ context.Context) error {
	nodes, err := nac.watchFactory.GetNodes()
	if err != nil {
		return fmt.Errorf("unable to list nodes: %w", err)
	}
	nodeObjs := make([]interface{}, len(nodes))
	for i, n := range nodes {
		nodeObjs[i] = n
	}
	if err := nac.syncNodes(nodeObjs); err != nil {
		return fmt.Errorf("unable to sync nodes: %w", err)
	}

	runCtx := wait.ContextForChannel(nac.stopChan)
	if err := nac.WatchNodes(runCtx, 1); err != nil {
		return fmt.Errorf("unable to watch nodes: %w", err)
	}
	return nil
}

func (nac *nodeAllocationController) Stop() {
	nac.stopOnce.Do(func() {
		close(nac.stopChan)
	})
	if nac.nodeQueue != nil {
		nac.nodeQueue.ShutDown()
	}
	nac.wg.Wait()

	if nac.nodeHandler != nil {
		_ = nac.watchFactory.NodeInformer().RemoveEventHandler(nac.nodeHandler)
		nac.nodeHandler = nil
	}
}

func needsNodeAllocation(node *corev1.Node) bool {
	if config.HybridOverlay.Enabled && util.NoHostSubnet(node) {
		// skip hybrid overlay nodes
		return false
	}

	if _, ok := node.Annotations[util.OvnNodeID]; !ok {
		return true
	}
	if _, ok := node.Annotations[util.OvnTransitSwitchPortAddr]; !ok {
		return true
	}
	return false
}

// handleAddUpdateNodeEvent handles the add or update node event
func (nac *nodeAllocationController) handleAddUpdateNodeEvent(node *corev1.Node) error {
	if config.HybridOverlay.Enabled && util.NoHostSubnet(node) {
		// skip hybrid overlay nodes
		return nil
	}
	allocatedNodeID, err := nac.nodeIDAllocator.AllocateID(node.Name)
	if err != nil {
		return fmt.Errorf("failed to allocate an id to the node %s : err - %w", node.Name, err)
	}
	klog.V(5).Infof("Allocated id %d to the node %s", allocatedNodeID, node.Name)
	nodeAnnotations := util.UpdateNodeIDAnnotation(nil, allocatedNodeID)

	// Allocate the IP address(es) for the node Gateway router port connecting
	// to the Join switch
	var v4Addr, v6Addr *net.IPNet

	if config.IPv4Mode {
		v4Addr, err = nac.transitSwitchIPv4Generator.GenerateIP(allocatedNodeID)
		if err != nil {
			return fmt.Errorf("failed to generate transit switch port IPv4 address for node %s : err - %w", node.Name, err)
		}
	}

	if config.IPv6Mode {
		v6Addr, err = nac.transitSwitchIPv6Generator.GenerateIP(allocatedNodeID)
		if err != nil {
			return fmt.Errorf("failed to generate transit switch port IPv6 address for node %s : err - %w", node.Name, err)
		}
	}

	nodeAnnotations, err = util.CreateNodeTransitSwitchPortAddrAnnotation(nodeAnnotations, v4Addr, v6Addr)
	if err != nil {
		return fmt.Errorf("failed to marshal node %q transit switch port IP annotation, err : %w",
			node.Name, err)
	}

	return nac.kube.SetAnnotationsOnNode(node.Name, nodeAnnotations)
}

// handleDeleteNode handles the delete node event.
func (nac *nodeAllocationController) handleDeleteNode(nodeName string) error {
	nac.nodeIDAllocator.ReleaseID(nodeName)
	return nil
}

func (nac *nodeAllocationController) syncNodes(nodes []interface{}) error {
	return nac.syncNodeIDs(nodes)
}

func (nac *nodeAllocationController) syncNodeIDs(nodes []interface{}) error {
	duplicateIdNodes := []string{}

	for _, nodeObj := range nodes {
		node, ok := nodeObj.(*corev1.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", nodeObj)
		}

		nodeID, _ := util.GetNodeID(node)
		if nodeID != util.InvalidNodeID {
			klog.Infof("Node %s has the id %d set", node.Name, nodeID)
			if err := nac.nodeIDAllocator.ReserveID(node.Name, nodeID); err != nil {
				// The id set on this node is duplicate.
				klog.Infof("Node %s has a duplicate id %d set", node.Name, nodeID)
				duplicateIdNodes = append(duplicateIdNodes, node.Name)
			}
		}
	}

	for i := range duplicateIdNodes {
		newNodeID, err := nac.nodeIDAllocator.AllocateID(duplicateIdNodes[i])
		if err != nil {
			return fmt.Errorf("failed to allocate id for node %s : err - %w", duplicateIdNodes[i], err)
		} else {
			klog.Infof("Allocated new id %d for node %q", newNodeID, duplicateIdNodes[i])
		}
	}

	return nil
}
