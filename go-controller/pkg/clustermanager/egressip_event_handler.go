// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package clustermanager

import (
	"fmt"
	"reflect"

	ocpcloudnetworkapi "github.com/openshift/api/cloudnetwork/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	egressipv1 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// egressIPClusterControllerEventHandler object handles the events
// for the egressIPClusterController.
type egressIPClusterControllerEventHandler struct {
	objType reflect.Type
	eIPC    *egressIPClusterController
}

// egressIPClusterControllerEventHandler functions

// AddResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
func (h *egressIPClusterControllerEventHandler) AddResource(obj interface{}, _ bool) error {
	switch h.objType {
	case factory.EgressNodeType:
		node := obj.(*corev1.Node)
		// EgressIP is not supported on hybrid overlay nodes
		if util.NoHostSubnet(node) {
			return nil
		}

		// Initialize the allocator on every update,
		// ovnkube-node/cloud-network-config-controller will make sure to
		// annotate the node with the egressIPConfig, but that might have
		// happened after we processed the ADD for that object, hence keep
		// retrying for all UPDATEs.
		if err := h.eIPC.initEgressIPAllocator(node); err != nil {
			klog.Warningf("Egress node initialization error: %v", err)
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
		if hasEgressLabel {
			// A node is only assignable when its host-cidrs annotation is
			// present, parseable, and non-empty; without addresses the
			// conflict check has no data for this node.
			hostCIDRs, err := util.ParseNodeHostCIDRsDropNetMask(node)
			nodeIsAssignable := err == nil && hostCIDRs.Len() > 0
			if err != nil {
				klog.Warningf("Node %s is not egress-assignable: failed to parse host-cidrs: %v", node.Name, err)
			} else if hostCIDRs.Len() == 0 {
				klog.Warningf("Node %s is not egress-assignable: host-cidrs contains no host CIDRs", node.Name)
			}
			h.eIPC.setNodeEgressAssignable(node.Name, nodeIsAssignable)
		}
		isReady := h.eIPC.isEgressNodeReady(node)
		if isReady {
			h.eIPC.setNodeEgressReady(node.Name, true)
		}
		isReachable := h.eIPC.isEgressNodeReachable(node)
		if hasEgressLabel && isReachable && isReady {
			h.eIPC.setNodeEgressReachable(node.Name, true)
			if err := h.eIPC.addEgressNode(node.Name); err != nil {
				return err
			}
		}
	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return h.eIPC.reconcileEgressIP(nil, eIP)
	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.eIPC.reconcileCloudPrivateIPConfig(nil, cloudPrivateIPConfig)
	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according
// to its type and returns the error, if any, yielded during the object update.
// The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
func (h *egressIPClusterControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) (err error) {
	switch h.objType {
	case factory.EgressIPType:
		var oldEIP *egressipv1.EgressIP
		if oldObj != nil {
			oldEIP = oldObj.(*egressipv1.EgressIP)
		}
		newEIP := newObj.(*egressipv1.EgressIP)
		return h.eIPC.reconcileEgressIP(oldEIP, newEIP)
	case factory.EgressNodeType:
		var oldNode *corev1.Node
		if oldObj != nil {
			oldNode = oldObj.(*corev1.Node)
		}
		newNode := newObj.(*corev1.Node)

		// EgressIP is not supported on hybrid overlay nodes
		if util.NoHostSubnet(newNode) {
			return nil
		}

		// Initialize the allocator on every update,
		// ovnkube-node/cloud-network-config-controller will make sure to
		// annotate the node with the egressIPConfig, but that might have
		// happened after we processed the ADD for that object, hence keep
		// retrying for all UPDATEs.
		isCloudEgressIPConfigAltered, isHostCIDRsAltered := h.eIPC.isEgressNodeConfigAltered(newNode)
		if initErr := h.eIPC.initEgressIPAllocator(newNode); initErr != nil {
			klog.Warningf("Egress node initialization error: %v", initErr)
		}
		defer func() {
			if err == nil {
				h.eIPC.commitEgressNodeConfig(newNode)
			}
		}()
		nodeEgressLabel := util.GetNodeEgressLabel()
		newLabels := newNode.GetLabels()
		_, newHasEgressLabel := newLabels[nodeEgressLabel]

		var oldHadEgressLabel, oldNodeIsAssignable, isOldReady bool
		if oldNode != nil {
			oldLabels := oldNode.GetLabels()
			_, oldHadEgressLabel = oldLabels[nodeEgressLabel]
			if oldHadEgressLabel {
				hostCIDRs, err := util.ParseNodeHostCIDRsDropNetMask(oldNode)
				oldNodeIsAssignable = err == nil && hostCIDRs.Len() > 0
			}
			isOldReady = h.eIPC.isEgressNodeReady(oldNode)
			if !isHostCIDRsAltered {
				isHostCIDRsAltered = util.NodeHostCIDRsAnnotationChanged(oldNode, newNode)
			}
			if !isCloudEgressIPConfigAltered {
				isCloudEgressIPConfigAltered = util.CloudEgressIPConfigAnnotationChanged(oldNode, newNode)
			}
		} else {
			h.eIPC.nodeAllocator.Lock()
			if eNode, exists := h.eIPC.nodeAllocator.cache[newNode.Name]; exists {
				oldHadEgressLabel = eNode.isEgressAssignable
				oldNodeIsAssignable = eNode.isEgressAssignable
				isOldReady = eNode.isReady
			}
			h.eIPC.nodeAllocator.Unlock()
		}

		// If the node is not labeled for egress assignment, just return
		// directly, we don't really need to set the ready / reachable
		// status on this node if the user doesn't care about using it.
		if !oldHadEgressLabel && !newHasEgressLabel {
			return nil
		}
		// A node is only assignable when it carries the egress-assignable label
		// and its host-cidrs annotation is present, parseable, and non-empty.
		// Without addresses the conflict check has no data for this node.
		nodeIsAssignable := newHasEgressLabel
		if nodeIsAssignable {
			hostCIDRs, err := util.ParseNodeHostCIDRsDropNetMask(newNode)
			nodeIsAssignable = err == nil && hostCIDRs.Len() > 0
			if err != nil {
				klog.Warningf("Node %s is not egress-assignable: failed to parse host-cidrs: %v", newNode.Name, err)
			} else if hostCIDRs.Len() == 0 {
				klog.Warningf("Node %s is not egress-assignable: host-cidrs contains no host CIDRs", newNode.Name)
			}
		}
		h.eIPC.setNodeEgressAssignable(newNode.Name, nodeIsAssignable)

		// setNodeEgressAssignable clears the node's allocation cache when it
		// turns a node unassignable, on the understanding that the caller then
		// clears that node's assignments too. Release them here to keep the
		// cache and status.items in agreement. A node that is genuinely gone is
		// released anyway by the readiness and reachability handling below, so
		// this only fires for a labelled, reachable node whose annotation stopped
		// parsing.
		if newHasEgressLabel && !nodeIsAssignable && (oldNodeIsAssignable || inRetryCache) {
			klog.Infof("Node: %s is no longer assignable (host-cidrs annotation missing or invalid), "+
				"deleting it from egress assignment", newNode.Name)
			h.eIPC.setNodeEgressReady(newNode.Name, h.eIPC.isEgressNodeReady(newNode))
			if err := h.eIPC.deleteEgressNode(newNode.Name); err != nil {
				return fmt.Errorf("failed to delete egress assignments for node %s: %w", newNode.Name, err)
			}
			return nil
		}

		if oldHadEgressLabel && !newHasEgressLabel {
			klog.Infof("Node: %s has been un-labeled, deleting it from egress assignment", newNode.Name)
			return h.eIPC.deleteEgressNode(newNode.Name)
		}
		isNewReady := h.eIPC.isEgressNodeReady(newNode)
		isNewReachable := h.eIPC.isEgressNodeReachable(newNode)
		h.eIPC.setNodeEgressReady(newNode.Name, isNewReady)
		if !oldHadEgressLabel && newHasEgressLabel {
			klog.Infof("Node: %s has been labeled, adding it for egress assignment", newNode.Name)
			if isNewReady && isNewReachable {
				h.eIPC.setNodeEgressReachable(newNode.Name, isNewReachable)
				if err := h.eIPC.addEgressNode(newNode.Name); err != nil {
					return err
				}
			} else {
				klog.Warningf("Node: %s has been labeled, but node is not ready"+
					" and reachable, cannot use it for egress assignment", newNode.Name)
			}
			return nil
		}
		if isOldReady == isNewReady && !isHostCIDRsAltered && !isCloudEgressIPConfigAltered {
			return nil
		}
		if !isNewReady {
			klog.Warningf("Node: %s is not ready, deleting it from egress assignment", newNode.Name)
			if err := h.eIPC.deleteEgressNode(newNode.Name); err != nil {
				return err
			}
		} else if isNewReady && isNewReachable {
			// Build a log message that captures all reasons we are re-evaluating,
			// so operators can correlate annotation changes with re-assignments even
			// when multiple conditions change simultaneously.
			switch {
			case isCloudEgressIPConfigAltered && isHostCIDRsAltered:
				klog.Infof("Node: %s cloud egress IP config annotation and host CIDRs changed, re-evaluating egress IP assignments", newNode.Name)
			case isCloudEgressIPConfigAltered:
				klog.Infof("Node: %s cloud egress IP config annotation changed, re-evaluating egress IP assignments", newNode.Name)
			default:
				klog.Infof("Node: %s is ready and reachable, adding it for egress assignment", newNode.Name)
			}
			h.eIPC.setNodeEgressReachable(newNode.Name, isNewReachable)
			if err := h.eIPC.addEgressNode(newNode.Name); err != nil {
				return err
			}
		}
		if isHostCIDRsAltered {
			// we only need to consider EIPs that are assigned to networks that aren't managed by OVN
			if err := h.eIPC.reconcileSecondaryHostNetworkEIPs(newNode); err != nil {
				return fmt.Errorf("failed to reconsider egress IPs that are secondary host networks: %v", err)
			}
		}
		return nil
	case factory.CloudPrivateIPConfigType:
		var oldCloudPrivateIPConfig *ocpcloudnetworkapi.CloudPrivateIPConfig
		if oldObj != nil {
			oldCloudPrivateIPConfig = oldObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		}
		newCloudPrivateIPConfig := newObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.eIPC.reconcileCloudPrivateIPConfig(oldCloudPrivateIPConfig, newCloudPrivateIPConfig)
	default:
		return fmt.Errorf("no update function for object type %s", h.objType)
	}
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// cachedObj is the internal cache entry for this object, used for now for pods and network policies.
func (h *egressIPClusterControllerEventHandler) DeleteResource(obj, _ interface{}) error {
	switch h.objType {
	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return h.eIPC.reconcileEgressIP(eIP, nil)
	case factory.EgressNodeType:
		node := obj.(*corev1.Node)
		// EgressIP is not supported on hybrid overlay nodes
		if util.NoHostSubnet(node) {
			return nil
		}
		h.eIPC.deleteNodeForEgress(node.Name)
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
		if hasEgressLabel {
			if err := h.eIPC.deleteEgressNode(node.Name); err != nil {
				return err
			}
		}
		return nil
	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.eIPC.reconcileCloudPrivateIPConfig(cloudPrivateIPConfig, nil)
	default:
		return fmt.Errorf("no delete function for object type %s", h.objType)
	}
}
