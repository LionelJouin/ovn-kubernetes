// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package clustermanager

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	ipamclaimsapi "github.com/k8snetworkplumbingwg/ipamclaims/pkg/crd/ipamclaims/v1alpha1"
	fakeipamclaimclient "github.com/k8snetworkplumbingwg/ipamclaims/pkg/crd/ipamclaims/v1alpha1/apis/clientset/versioned/fake"
	"github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	nodecontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controllers/node"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/persistentips"
	ovntest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func TestHandleNetworkRefChangeUpdatesStatusAndMetrics(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true

	metrics.RegisterClusterManagerFunctional()

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_refchange_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.1.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()
	defer metrics.DeleteDynamicUDNNodeCount(networkName)

	var gotNetwork string
	var gotCondStatus string
	var gotCondMsg string
	fakeClient := util.GetOVNClientset().GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()
	nm := &networkmanager.FakeNetworkManager{}
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()
	ncc := &networkClusterController{
		ReconcilableNetInfo: util.NewReconcilableNetInfo(netInfo),
		nodeReconciler:      nodeController,
		statusReporter: func(networkName, _ string, condition *metav1.Condition, _ ...*util.EventDetails) error {
			gotNetwork = networkName
			if condition != nil {
				gotCondStatus = string(condition.Status)
				gotCondMsg = condition.Message
			}
			return nil
		},
	}

	ncc.HandleNetworkRefChange("node1", true)
	g.Expect(gotNetwork).To(gomega.Equal(networkName))
	g.Expect(gotCondStatus).To(gomega.Equal(string(metav1.ConditionTrue)))
	g.Expect(gotCondMsg).To(gomega.Equal("1 node(s) rendered with network"))
	g.Expect(getUDNNodesRenderedMetric(t, networkName)).To(gomega.Equal(1.0))

	ncc.HandleNetworkRefChange("node1", true)
	g.Expect(gotNetwork).To(gomega.Equal(networkName))
	g.Expect(gotCondStatus).To(gomega.Equal(string(metav1.ConditionTrue)))
	g.Expect(gotCondMsg).To(gomega.Equal("1 node(s) rendered with network"))
	g.Expect(getUDNNodesRenderedMetric(t, networkName)).To(gomega.Equal(1.0))

	ncc.HandleNetworkRefChange("node2", true)
	g.Expect(gotNetwork).To(gomega.Equal(networkName))
	g.Expect(gotCondStatus).To(gomega.Equal(string(metav1.ConditionTrue)))
	g.Expect(gotCondMsg).To(gomega.Equal("2 node(s) rendered with network"))
	g.Expect(getUDNNodesRenderedMetric(t, networkName)).To(gomega.Equal(2.0))

	ncc.HandleNetworkRefChange("node2", false)
	g.Expect(gotNetwork).To(gomega.Equal(networkName))
	g.Expect(gotCondStatus).To(gomega.Equal(string(metav1.ConditionTrue)))
	g.Expect(gotCondMsg).To(gomega.Equal("1 node(s) rendered with network"))
	g.Expect(getUDNNodesRenderedMetric(t, networkName)).To(gomega.Equal(1.0))

	ncc.HandleNetworkRefChange("node1", false)
	ncc.HandleNetworkRefChange("node1", false)
	g.Expect(gotCondStatus).To(gomega.Equal(string(metav1.ConditionFalse)))
	g.Expect(gotCondMsg).To(gomega.Equal("no nodes currently rendered with network"))
	g.Expect(getUDNNodesRenderedMetric(t, networkName)).To(gomega.Equal(0.0))
}

func TestHandleNetworkRefChangeCleanupWithZeroGraceOnStart(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true
	config.OVNKubernetesFeature.UDNDeletionGracePeriod = 0

	metrics.RegisterClusterManagerFunctional()

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_cleanup_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.1.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()
	defer metrics.DeleteDynamicUDNNodeCount(networkName)

	nodeSubnet := ovntest.MustParseIPNet("10.1.0.0/24")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{},
		},
	}
	node.Annotations, err = util.UpdateNodeHostSubnetAnnotation(node.Annotations, []*net.IPNet{nodeSubnet}, networkName)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	node.Annotations, err = util.UpdateNetworkIDAnnotation(node.Annotations, networkName, 7)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	fakeClient := util.GetOVNClientset(node).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()
	nm := &networkmanager.FakeNetworkManager{}
	nm.SetNodeActive(networkName, node.Name, false)
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	g.Expect(ncc.Start(context.Background())).To(gomega.Succeed())
	defer ncc.Stop()

	g.Eventually(func() bool {
		updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if util.HasNodeHostSubnetAnnotation(updatedNode, networkName) {
			return false
		}
		_, err = util.ParseNetworkIDAnnotation(updatedNode, networkName)
		return util.IsAnnotationNotSetError(err)
	}).Should(gomega.BeTrue())
}

func TestHandleNetworkRefChangeCleanupWithZeroGraceAfterStart(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true
	config.OVNKubernetesFeature.UDNDeletionGracePeriod = 0

	metrics.RegisterClusterManagerFunctional()

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_cleanup_after_start_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.2.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()
	defer metrics.DeleteDynamicUDNNodeCount(networkName)

	nodeSubnet := ovntest.MustParseIPNet("10.2.0.0/24")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{},
		},
	}
	node.Annotations, err = util.UpdateNodeHostSubnetAnnotation(node.Annotations, []*net.IPNet{nodeSubnet}, networkName)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	node.Annotations, err = util.UpdateNetworkIDAnnotation(node.Annotations, networkName, 7)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	fakeClient := util.GetOVNClientset(node).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()
	nm := &networkmanager.FakeNetworkManager{}
	nm.SetNodeActive(networkName, node.Name, true)
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	g.Expect(ncc.Start(context.Background())).To(gomega.Succeed())
	defer ncc.Stop()

	updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(util.HasNodeHostSubnetAnnotation(updatedNode, networkName)).To(gomega.BeTrue())

	nm.SetNodeActive(networkName, node.Name, false)
	ncc.HandleNetworkRefChange(node.Name, false)

	g.Eventually(func() bool {
		updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if util.HasNodeHostSubnetAnnotation(updatedNode, networkName) {
			return false
		}
		_, err = util.ParseNetworkIDAnnotation(updatedNode, networkName)
		return util.IsAnnotationNotSetError(err)
	}).Should(gomega.BeTrue())
}

func TestHandleNetworkRefChangeAllocatesOnActivation(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_activate_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.10.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()
	defer metrics.DeleteDynamicUDNNodeCount(networkName)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{},
		},
	}

	fakeClient := util.GetOVNClientset(node).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()
	nm := &networkmanager.FakeNetworkManager{}
	nm.SetNodeActive(networkName, node.Name, false)
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	g.Expect(ncc.Start(context.Background())).To(gomega.Succeed())
	defer ncc.Stop()

	// Ensure node does not get allocated while inactive.
	g.Consistently(func() bool {
		updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return util.HasNodeHostSubnetAnnotation(updatedNode, networkName)
	}, time.Second, 50*time.Millisecond).Should(gomega.BeFalse())

	// Activate and verify allocation occurs via retry framework.
	nm.SetNodeActive(networkName, node.Name, true)
	ncc.HandleNetworkRefChange(node.Name, true)

	g.Eventually(func() bool {
		updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return util.HasNodeHostSubnetAnnotation(updatedNode, networkName)
	}).Should(gomega.BeTrue())
}

func TestReconcileNodeCleansUpOnNoHostSubnetTransition(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true

	origNoHostSubnetNodes := config.Kubernetes.NoHostSubnetNodes
	config.Kubernetes.NoHostSubnetNodes, err = metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"no-host-subnet": "true"},
	})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	t.Cleanup(func() {
		config.Kubernetes.NoHostSubnetNodes = origNoHostSubnetNodes
	})

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_no_host_subnet_transition_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.20.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()

	nodeSubnet := ovntest.MustParseIPNet("10.20.0.0/24")
	oldNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{},
		},
	}
	oldNode.Annotations, err = util.UpdateNodeHostSubnetAnnotation(oldNode.Annotations, []*net.IPNet{nodeSubnet}, networkName)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	oldNode.Annotations, err = util.UpdateNetworkIDAnnotation(oldNode.Annotations, networkName, 7)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	newNode := oldNode.DeepCopy()
	newNode.Labels = map[string]string{"no-host-subnet": "true"}

	fakeClient := util.GetOVNClientset(newNode).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()

	nm := &networkmanager.FakeNetworkManager{}
	nm.SetNodeActive(networkName, newNode.Name, false)
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())

	g.Expect(ncc.ReconcileNode(oldNode, newNode, nil, nil)).To(gomega.Succeed())

	updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), newNode.Name, metav1.GetOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(util.HasNodeHostSubnetAnnotation(updatedNode, networkName)).To(gomega.BeFalse())
	_, err = util.ParseNetworkIDAnnotation(updatedNode, networkName)
	g.Expect(util.IsAnnotationNotSetError(err)).To(gomega.BeTrue())
}

func TestReconcileNodeMarksNodeSyncFailedOnCleanupError(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_cleanup_error_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer3Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.30.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Annotations: map[string]string{
				util.OvnNetworkIDs: "{broken-json",
			},
		},
	}

	fakeClient := util.GetOVNClientset(node).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()

	nm := &networkmanager.FakeNetworkManager{}
	nm.SetNodeActive(networkName, node.Name, false)
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())

	err = ncc.ReconcileNode(nil, node, nil, nil)
	g.Expect(err).To(gomega.HaveOccurred())
	_, failed := ncc.nodeSyncFailed.Load(node.Name)
	g.Expect(failed).To(gomega.BeTrue())
	_, parseErr := util.ParseNetworkIDAnnotation(node, networkName)
	g.Expect(parseErr).To(gomega.HaveOccurred())
}

func getUDNNodesRenderedMetric(t *testing.T, networkName string) float64 {
	t.Helper()

	metricName := fmt.Sprintf("%s_%s_%s",
		types.MetricOvnkubeNamespace,
		types.MetricOvnkubeSubsystemClusterManager,
		"udn_nodes_rendered",
	)
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if labelValue(metric.GetLabel(), "network_name") != networkName {
				continue
			}
			if metric.GetGauge() == nil {
				t.Fatalf("metric %s for %s is not a gauge", metricName, networkName)
			}
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s with network_name=%s not found", metricName, networkName)
	return 0
}

func labelValue(labels []*dto.LabelPair, name string) string {
	for _, label := range labels {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func TestNetworkClusterControllerInitHandlersIdempotent(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_idempotent_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer2Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.50.0.0/16",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	fakeClient := util.GetOVNClientset().GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()

	nm := &networkmanager.FakeNetworkManager{}
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		nil,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	firstQueue := ncc.podQueue
	firstHandler := ncc.podHandler
	g.Expect(firstQueue).ToNot(gomega.BeNil())
	g.Expect(firstHandler).ToNot(gomega.BeNil())

	// Call initHandlers again to verify it is idempotent
	g.Expect(ncc.initHandlers()).To(gomega.Succeed())
	g.Expect(ncc.podQueue).To(gomega.Equal(firstQueue))
	g.Expect(ncc.podHandler).To(gomega.Equal(firstHandler))
}

func TestNetworkClusterControllerPodWorkqueueReconciliation(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_pod_wq_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology: types.Layer2Topology,
		Role:     types.NetworkRolePrimary,
		Subnets:  "10.60.0.0/24",
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       k8stypes.UID("uid-1"),
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}

	fakeClient := util.GetOVNClientset(node, pod).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()

	fakeRecorder := record.NewFakeRecorder(10)
	nm := &networkmanager.FakeNetworkManager{
		PrimaryNetworks: map[string]util.NetInfo{"default": netInfo},
		NADNetworks:     map[string]util.NetInfo{"default/nad": netInfo},
	}
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		fakeRecorder,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	g.Expect(ncc.Start(context.Background())).To(gomega.Succeed())
	defer ncc.Stop()

	// 1. Verify pod is allocated asynchronously by the worker
	g.Eventually(func() bool {
		updatedPod, err := fakeClient.KubeClient.CoreV1().Pods("default").Get(context.TODO(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		podAnnotation, err := util.UnmarshalPodAnnotation(updatedPod.Annotations, "default/nad")
		return err == nil && len(podAnnotation.IPs) > 0
	}, 3*time.Second).Should(gomega.BeTrue())

	allocatedPod, err := fakeClient.KubeClient.CoreV1().Pods("default").Get(context.TODO(), pod.Name, metav1.GetOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	_, err = util.UnmarshalPodAnnotation(allocatedPod.Annotations, "default/nad")
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// 2. Test pod recreation with same name and new UID
	podV2 := pod.DeepCopy()
	podV2.UID = k8stypes.UID("uid-2")
	podV2.Annotations = nil
	_, err = fakeClient.KubeClient.CoreV1().Pods("default").Update(context.TODO(), podV2, metav1.UpdateOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())

	g.Eventually(func() string {
		updatedPod, err := fakeClient.KubeClient.CoreV1().Pods("default").Get(context.TODO(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		podAnnotation, err := util.UnmarshalPodAnnotation(updatedPod.Annotations, "default/nad")
		if err != nil || len(podAnnotation.IPs) == 0 {
			return ""
		}
		return podAnnotation.IPs[0].IP.String()
	}, 3*time.Second).ShouldNot(gomega.BeEmpty())

	// 3. Test pod delete via tombstone (DeletedFinalStateUnknown)
	latestPod, err := fakeClient.KubeClient.CoreV1().Pods("default").Get(context.TODO(), pod.Name, metav1.GetOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	latestAnnot, err := util.UnmarshalPodAnnotation(latestPod.Annotations, "default/nad")
	g.Expect(err).ToNot(gomega.HaveOccurred())

	err = fakeClient.KubeClient.CoreV1().Pods("default").Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())

	tombstone := cache.DeletedFinalStateUnknown{
		Key: "default/" + pod.Name,
		Obj: latestPod,
	}
	ncc.onPodDelete(tombstone)

	key := "default/" + pod.Name
	g.Eventually(func() bool {
		_, cached := ncc.podCache.Load(key)
		return cached
	}, 3*time.Second).Should(gomega.BeFalse())

	// Verify the previously allocated IP can now be allocated again to another owner
	g.Expect(ncc.subnetAllocator.AllocateIPs(networkName, "new-owner", latestAnnot.IPs)).To(gomega.Succeed())
}

func TestNetworkClusterControllerIPAMClaimWorkqueueReconciliation(t *testing.T) {
	g := gomega.NewWithT(t)

	err := config.PrepareTestConfig()
	g.Expect(err).ToNot(gomega.HaveOccurred())
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnablePersistentIPs = true

	netConf := &ovncnitypes.NetConf{
		NetConf: cnitypes.NetConf{
			Name: "ns1_udn1_claim_wq_test",
			Type: "ovn-k8s-cni-overlay",
		},
		Topology:           types.Layer2Topology,
		Role:               types.NetworkRolePrimary,
		Subnets:            "10.70.0.0/24",
		AllowPersistentIPs: true,
	}
	netInfo, err := util.NewNetInfo(netConf)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	networkName := netInfo.GetNetworkName()

	claimName := "test-claim"
	namespace := "default"
	allocatedIP := "10.70.0.5/24"
	claim := &ipamclaimsapi.IPAMClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: namespace,
			UID:       k8stypes.UID("claim-uid-1"),
		},
		Spec: ipamclaimsapi.IPAMClaimSpec{
			Network: networkName,
		},
		Status: ipamclaimsapi.IPAMClaimStatus{
			IPs: []string{allocatedIP},
		},
	}

	fakeClient := util.GetOVNClientset().GetClusterManagerClientset()
	fakeClient.IPAMClaimsClient = fakeipamclaimclient.NewSimpleClientset(claim)
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(wf.Start()).To(gomega.Succeed())
	defer wf.Shutdown()

	fakeRecorder := record.NewFakeRecorder(10)
	nm := &networkmanager.FakeNetworkManager{
		PrimaryNetworks: map[string]util.NetInfo{"default": netInfo},
		NADNetworks:     map[string]util.NetInfo{"default/nad": netInfo},
	}
	nodeController := nodecontroller.NewController(wf, "clustermanager-node", nm)
	g.Expect(nodeController.Start()).To(gomega.Succeed())
	defer nodeController.Stop()

	ncc := newNetworkClusterController(
		netInfo,
		fakeClient,
		wf,
		fakeRecorder,
		nm,
		nil,
		nodeController,
	)
	g.Expect(ncc.init()).To(gomega.Succeed())
	g.Expect(ncc.Start(context.Background())).To(gomega.Succeed())
	defer ncc.Stop()

	key := namespace + "/" + claimName

	// 1. Verify IPAMClaim is synced and cached by the controller on start
	g.Eventually(func() bool {
		val, cached := ncc.ipamClaimCache.Load(key)
		if !cached || val == nil {
			return false
		}
		c := val.(*ipamclaimsapi.IPAMClaim)
		return len(c.Status.IPs) > 0 && c.Status.IPs[0] == allocatedIP
	}, 3*time.Second).Should(gomega.BeTrue())

	// Verify that the allocated IP is reserved in the allocator
	parsedIPs, err := util.ParseIPNets([]string{allocatedIP})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(ncc.subnetAllocator.AllocateIPs(networkName, "other-owner", parsedIPs)).ToNot(gomega.Succeed())

	// 2. Test IPAMClaim replacement with same name and new UID
	claimV2 := claim.DeepCopy()
	claimV2.UID = k8stypes.UID("claim-uid-2")
	claimV2.Status.IPs = []string{"10.70.0.6/24"}
	_, err = fakeClient.IPAMClaimsClient.K8sV1alpha1().IPAMClaims(namespace).Update(context.TODO(), claimV2, metav1.UpdateOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())

	g.Eventually(func() string {
		val, cached := ncc.ipamClaimCache.Load(key)
		if !cached || val == nil {
			return ""
		}
		c := val.(*ipamclaimsapi.IPAMClaim)
		if c.UID != k8stypes.UID("claim-uid-2") || len(c.Status.IPs) == 0 {
			return ""
		}
		return c.Status.IPs[0]
	}, 3*time.Second).Should(gomega.Equal("10.70.0.6/24"))

	// The replaced claim's IP (10.70.0.5/24) must be released.
	g.Eventually(func() error {
		return ncc.subnetAllocator.AllocateIPs(networkName, "replacement-check", parsedIPs)
	}, 3*time.Second).Should(gomega.Succeed())
	g.Expect(ncc.subnetAllocator.ReleaseIPs(networkName, "replacement-check", parsedIPs)).To(gomega.Succeed())

	// Reserve the new claim's IP so the delete path has something to release.
	parsedIP2, err := util.ParseIPNets([]string{"10.70.0.6/24"})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(ncc.subnetAllocator.AllocateIPs(networkName, persistentips.IPAMClaimOwner(claimV2), parsedIP2)).To(gomega.Succeed())

	// 3. Test IPAMClaim delete via tombstone (DeletedFinalStateUnknown)
	latestClaim, err := fakeClient.IPAMClaimsClient.K8sV1alpha1().IPAMClaims(namespace).Get(context.TODO(), claimName, metav1.GetOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())
	err = fakeClient.IPAMClaimsClient.K8sV1alpha1().IPAMClaims(namespace).Delete(context.TODO(), claimName, metav1.DeleteOptions{})
	g.Expect(err).ToNot(gomega.HaveOccurred())

	tombstone := cache.DeletedFinalStateUnknown{
		Key: key,
		Obj: latestClaim,
	}
	ncc.onIPAMClaimDelete(tombstone)

	g.Eventually(func() bool {
		_, cached := ncc.ipamClaimCache.Load(key)
		return cached
	}, 3*time.Second).Should(gomega.BeFalse())

	// Verify the released IP can now be allocated again
	g.Expect(ncc.subnetAllocator.AllocateIPs(networkName, "new-claim-owner", parsedIP2)).To(gomega.Succeed())
}
