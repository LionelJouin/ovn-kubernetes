// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/safchain/ethtool"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	egressipv1 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/informer"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/node/bridgeconfig"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/node/egressip"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/errors"
)

// Gateway responds to Service and Endpoint K8s events
// and programs OVN gateway functionality.
// It may also spawn threads to ensure the flow tables
// are kept in sync
type Gateway interface {
	informer.ServiceAndEndpointsEventHandler
	Init(<-chan struct{}, *sync.WaitGroup) error
	Start() error
	GetGatewayBridgeIface() string
	GetGatewayIface() string
	GetUplinkName() string
	SetDefaultGatewayBridgeMAC(addr net.HardwareAddr)
	SetDefaultPodNetworkAdvertised(bool)
	SetDefaultBridgeGARPDropFlows(bool)
	Reconcile() error
	Stop()
}

type gateway struct {
	// loadBalancerHealthChecker is a health check server for load-balancer type services
	loadBalancerHealthChecker informer.ServiceAndEndpointsEventHandler
	// portClaimWatcher is for reserving ports for virtual IPs allocated by the cluster on the host
	portClaimWatcher informer.ServiceEventHandler
	// nodePortWatcherNFTables is used in Shared GW mode to handle nodePort nftables rules
	nodePortWatcherNFTables informer.ServiceEventHandler
	// nodePortWatcher is used in Local+Shared GW modes to handle nodePort flows in shared OVS bridge
	nodePortWatcher      informer.ServiceAndEndpointsEventHandler
	openflowManager      *openflowManager
	nodeIPManager        *addressManager
	bridgeEIPAddrManager *egressip.BridgeEIPAddrManager
	initFunc             func() error
	readyFunc            func() (bool, error)

	serviceQueue   workqueue.TypedRateLimitingInterface[string]
	serviceHandler cache.ResourceEventHandlerRegistration
	serviceResync  sync.Map // map[string]struct{}

	endpointSliceQueue   workqueue.TypedRateLimitingInterface[string]
	endpointSliceHandler cache.ResourceEventHandlerRegistration

	egressIPQueue   workqueue.TypedRateLimitingInterface[string]
	egressIPHandler cache.ResourceEventHandlerRegistration

	stopOnce     sync.Once
	watchFactory *factory.WatchFactory
	stopChan     <-chan struct{}
	wg           *sync.WaitGroup

	nextHops []net.IP
}

func (g *gateway) AddService(svc *corev1.Service) error {
	var err error
	var errors []error

	if g.portClaimWatcher != nil {
		if err = g.portClaimWatcher.AddService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.loadBalancerHealthChecker != nil {
		if err = g.loadBalancerHealthChecker.AddService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.AddService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcherNFTables != nil {
		if err = g.nodePortWatcherNFTables.AddService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)
}

func (g *gateway) UpdateService(old, new *corev1.Service) error {
	var err error
	var errors []error

	if g.portClaimWatcher != nil {
		if err = g.portClaimWatcher.UpdateService(old, new); err != nil {
			errors = append(errors, err)
		}
	}
	if g.loadBalancerHealthChecker != nil {
		if err = g.loadBalancerHealthChecker.UpdateService(old, new); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.UpdateService(old, new); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcherNFTables != nil {
		if err = g.nodePortWatcherNFTables.UpdateService(old, new); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)
}

func (g *gateway) DeleteService(svc *corev1.Service) error {
	var err error
	var errors []error

	if g.portClaimWatcher != nil {
		if err = g.portClaimWatcher.DeleteService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.loadBalancerHealthChecker != nil {
		if err = g.loadBalancerHealthChecker.DeleteService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.DeleteService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	if g.nodePortWatcherNFTables != nil {
		if err = g.nodePortWatcherNFTables.DeleteService(svc); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)
}

func (g *gateway) SyncServices(objs []interface{}) error {
	var err error
	klog.Infof("Starting gateway service sync")
	start := time.Now()
	if g.portClaimWatcher != nil {
		err = g.portClaimWatcher.SyncServices(objs)
	}
	if err == nil && g.loadBalancerHealthChecker != nil {
		err = g.loadBalancerHealthChecker.SyncServices(objs)
	}
	if err == nil && g.nodePortWatcher != nil {
		err = g.nodePortWatcher.SyncServices(objs)
	}
	if err == nil && g.nodePortWatcherNFTables != nil {
		err = g.nodePortWatcherNFTables.SyncServices(objs)
	}
	if err != nil {
		return fmt.Errorf("gateway sync services failed: %v", err)
	}
	klog.Infof("Gateway service sync done. Time taken: %s", time.Since(start))
	return nil
}

func (g *gateway) AddEndpointSlice(epSlice *discovery.EndpointSlice) error {
	var err error
	var errors []error

	if g.loadBalancerHealthChecker != nil {
		// Filter out objects without the default serviceName label to exclude mirrored EndpointSlices
		// Only default EndpointSlices contain the discovery.LabelServiceName label
		if !util.IsNetworkSegmentationSupportEnabled() || epSlice.Labels[discovery.LabelServiceName] != "" {
			if err = g.loadBalancerHealthChecker.AddEndpointSlice(epSlice); err != nil {
				errors = append(errors, err)
			}
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.AddEndpointSlice(epSlice); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)

}

func (g *gateway) UpdateEndpointSlice(oldEpSlice, newEpSlice *discovery.EndpointSlice) error {
	var err error
	var errors []error

	if g.loadBalancerHealthChecker != nil {
		// Filter out objects without the default serviceName label to exclude mirrored EndpointSlices
		// Only default EndpointSlices contain the discovery.LabelServiceName label
		if !util.IsNetworkSegmentationSupportEnabled() || newEpSlice.Labels[discovery.LabelServiceName] != "" {
			if err = g.loadBalancerHealthChecker.UpdateEndpointSlice(oldEpSlice, newEpSlice); err != nil {
				errors = append(errors, err)
			}
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.UpdateEndpointSlice(oldEpSlice, newEpSlice); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)

}

func (g *gateway) DeleteEndpointSlice(epSlice *discovery.EndpointSlice) error {
	var err error
	var errors []error

	if g.loadBalancerHealthChecker != nil {
		// Filter out objects without the default serviceName label to exclude mirrored EndpointSlices
		// Only default EndpointSlices contain the discovery.LabelServiceName label
		if !util.IsNetworkSegmentationSupportEnabled() || epSlice.Labels[discovery.LabelServiceName] != "" {
			if err = g.loadBalancerHealthChecker.DeleteEndpointSlice(epSlice); err != nil {
				errors = append(errors, err)
			}
		}
	}
	if g.nodePortWatcher != nil {
		if err = g.nodePortWatcher.DeleteEndpointSlice(epSlice); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)
}

// canHandleBridgeEgressIP returns true if this node should handle EgressIP
// configuration on the bridge. Returns false if:
// - Network segmentation (UDN) is not enabled
// - Gateway mode is disabled
// - Running in DPU-host mode (EgressIP is handled by ovnkube on the DPU where OVS runs)
func canHandleBridgeEgressIP() bool {
	return util.IsNetworkSegmentationSupportEnabled() &&
		config.Gateway.Mode != config.GatewayModeDisabled &&
		(config.IsModeDPU() || config.IsModeFull())
}

func (g *gateway) AddEgressIP(eip *egressipv1.EgressIP) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	isSyncRequired, err := g.bridgeEIPAddrManager.AddEgressIP(eip)
	if err != nil {
		return err
	}
	if isSyncRequired {
		if err = g.Reconcile(); err != nil {
			return fmt.Errorf("failed to sync gateway: %v", err)
		}
	}
	return nil
}

func (g *gateway) UpdateEgressIP(oldEIP, newEIP *egressipv1.EgressIP) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	isSyncRequired, err := g.bridgeEIPAddrManager.UpdateEgressIP(oldEIP, newEIP)
	if err != nil {
		return err
	}
	if isSyncRequired {
		if err = g.Reconcile(); err != nil {
			return fmt.Errorf("failed to sync gateway: %v", err)
		}
	}
	return nil
}

func (g *gateway) DeleteEgressIP(eip *egressipv1.EgressIP) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	isSyncRequired, err := g.bridgeEIPAddrManager.DeleteEgressIP(eip)
	if err != nil {
		return err
	}
	if isSyncRequired {
		if err = g.Reconcile(); err != nil {
			return fmt.Errorf("failed to sync gateway: %v", err)
		}
	}
	return nil
}

func (g *gateway) DeleteEgressIPByName(name string) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	isSyncRequired, err := g.bridgeEIPAddrManager.DeleteEgressIPByName(name)
	if err != nil {
		return err
	}
	if isSyncRequired {
		if err = g.Reconcile(); err != nil {
			return fmt.Errorf("failed to sync gateway: %v", err)
		}
	}
	return nil
}

func (g *gateway) EnsureEgressIP(eip *egressipv1.EgressIP) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	isSyncRequired, err := g.bridgeEIPAddrManager.EnsureEgressIP(eip)
	if err != nil {
		return err
	}
	if isSyncRequired {
		if err = g.Reconcile(); err != nil {
			return fmt.Errorf("failed to sync gateway: %v", err)
		}
	}
	return nil
}

func (g *gateway) SyncEgressIP(eips []interface{}) error {
	if !canHandleBridgeEgressIP() {
		return nil
	}
	if err := g.bridgeEIPAddrManager.SyncEgressIP(eips); err != nil {
		return err
	}
	if err := g.Reconcile(); err != nil {
		return fmt.Errorf("failed to sync gateway: %v", err)
	}
	return nil
}

func (g *gateway) onServiceAdd(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(svc)
	if err != nil {
		return
	}
	g.serviceQueue.Add(key)
}

func (g *gateway) onServiceUpdate(oldObj, newObj interface{}) {
	oldSvc, ok1 := oldObj.(*corev1.Service)
	newSvc, ok2 := newObj.(*corev1.Service)
	if !ok1 || !ok2 {
		return
	}
	if serviceUpdateNotNeeded(oldSvc, newSvc) {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(newSvc)
	if err != nil {
		return
	}
	g.serviceQueue.Add(key)
}

func (g *gateway) onServiceDelete(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		svc, ok = tombstone.Obj.(*corev1.Service)
		if !ok {
			return
		}
	}
	key, err := cache.MetaNamespaceKeyFunc(svc)
	if err != nil {
		return
	}
	g.serviceQueue.Add(key)
}

func (g *gateway) runServiceWorker(ctx context.Context) {
	for g.processServiceItem() {
	}
}

func (g *gateway) processServiceItem() bool {
	key, shutdown := g.serviceQueue.Get()
	if shutdown {
		return false
	}
	defer g.serviceQueue.Done(key)

	err := g.reconcileService(key)
	if err != nil {
		if g.serviceQueue.NumRequeues(key) < controller.DefaultMaxAttempts {
			klog.V(4).Infof("Error reconciling service %s: %v, requeuing", key, err)
			g.serviceQueue.AddRateLimited(key)
			return true
		}
		klog.Errorf("Dropping service %s out of gateway queue after %d attempts: %v", key, controller.DefaultMaxAttempts, err)
		g.serviceResync.Delete(key)
	}
	g.serviceQueue.Forget(key)
	return true
}

func (g *gateway) reconcileService(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("failed to split key %s: %w", key, err)
	}

	curSvc, err := g.watchFactory.GetService(namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			g.serviceResync.Delete(key)
			dummySvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
				},
			}
			return g.DeleteService(dummySvc)
		}
		return fmt.Errorf("failed to get service %s: %w", key, err)
	}

	_, forceAdd := g.serviceResync.LoadAndDelete(key)
	if forceAdd {
		err = g.AddService(curSvc)
	} else {
		err = g.UpdateService(nil, curSvc)
	}
	if err != nil {
		if forceAdd {
			g.serviceResync.Store(key, struct{}{})
		}
		return err
	}
	return nil
}

func endpointSliceUpdateNotNeeded(oldEPS, newEPS *discovery.EndpointSlice) bool {
	return reflect.DeepEqual(oldEPS.Endpoints, newEPS.Endpoints) &&
		reflect.DeepEqual(oldEPS.Ports, newEPS.Ports) &&
		reflect.DeepEqual(oldEPS.Labels, newEPS.Labels) &&
		reflect.DeepEqual(oldEPS.Annotations, newEPS.Annotations)
}

func egressIPUpdateNotNeeded(oldEIP, newEIP *egressipv1.EgressIP) bool {
	return reflect.DeepEqual(oldEIP.Status.Items, newEIP.Status.Items) &&
		reflect.DeepEqual(oldEIP.Spec.EgressIPs, newEIP.Spec.EgressIPs) &&
		!util.EgressIPMarkAnnotationChanged(oldEIP.Annotations, newEIP.Annotations)
}

func (g *gateway) onEndpointSliceAdd(obj interface{}) {
	eps, ok := obj.(*discovery.EndpointSlice)
	if !ok {
		return
	}
	g.enqueueEndpointSliceService(eps)
}

func (g *gateway) onEndpointSliceUpdate(oldObj, newObj interface{}) {
	oldEPS, ok1 := oldObj.(*discovery.EndpointSlice)
	newEPS, ok2 := newObj.(*discovery.EndpointSlice)
	if !ok1 || !ok2 {
		return
	}
	if endpointSliceUpdateNotNeeded(oldEPS, newEPS) {
		return
	}
	oldSvc := getServiceNameFromEndpointSlice(oldEPS)
	newSvc := getServiceNameFromEndpointSlice(newEPS)
	if oldSvc != "" && oldSvc != newSvc {
		g.endpointSliceQueue.Add(oldEPS.Namespace + "/" + oldSvc)
	}
	g.enqueueEndpointSliceService(newEPS)
}

func (g *gateway) onEndpointSliceDelete(obj interface{}) {
	eps, ok := obj.(*discovery.EndpointSlice)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		eps, ok = tombstone.Obj.(*discovery.EndpointSlice)
		if !ok {
			return
		}
	}
	g.enqueueEndpointSliceService(eps)
}

func (g *gateway) enqueueEndpointSliceService(eps *discovery.EndpointSlice) {
	svcName := getServiceNameFromEndpointSlice(eps)
	if svcName == "" {
		return
	}
	g.endpointSliceQueue.Add(eps.Namespace + "/" + svcName)
}

func getServiceNameFromEndpointSlice(eps *discovery.EndpointSlice) string {
	if svcName, ok := eps.Labels[discovery.LabelServiceName]; ok && svcName != "" {
		return svcName
	}
	if svcName, ok := eps.Labels[types.LabelUserDefinedServiceName]; ok && svcName != "" {
		return svcName
	}
	return ""
}

func (g *gateway) runEndpointSliceWorker(_ context.Context) {
	for g.processEndpointSliceItem() {
	}
}

func (g *gateway) processEndpointSliceItem() bool {
	key, shutdown := g.endpointSliceQueue.Get()
	if shutdown {
		return false
	}
	defer g.endpointSliceQueue.Done(key)

	err := g.reconcileEndpointSlice(key)
	if err != nil {
		if g.endpointSliceQueue.NumRequeues(key) < controller.DefaultMaxAttempts {
			klog.V(4).Infof("Error reconciling endpoint slice %s: %v, requeuing", key, err)
			g.endpointSliceQueue.AddRateLimited(key)
			return true
		}
		klog.Errorf("Dropping endpoint slice %s out of gateway queue after %d attempts: %v", key, controller.DefaultMaxAttempts, err)
	}
	g.endpointSliceQueue.Forget(key)
	return true
}

func (g *gateway) reconcileEndpointSlice(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("failed to split key %s: %w", key, err)
	}

	curSvc, err := g.watchFactory.GetService(namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get service %s: %w", key, err)
	}

	return g.SyncEndpointSlicesForService(curSvc)
}

type serviceEndpointSliceSyncer interface {
	SyncEndpointSlicesForService(*corev1.Service) error
}

func (g *gateway) SyncEndpointSlicesForService(svc *corev1.Service) error {
	var errors []error
	if syncable, ok := g.loadBalancerHealthChecker.(serviceEndpointSliceSyncer); ok {
		if err := syncable.SyncEndpointSlicesForService(svc); err != nil {
			errors = append(errors, err)
		}
	} else if g.loadBalancerHealthChecker != nil {
		dummyEPS := &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: svc.Namespace,
				Name:      svc.Name,
				Labels: map[string]string{
					discovery.LabelServiceName: svc.Name,
				},
			},
		}
		if err := g.loadBalancerHealthChecker.AddEndpointSlice(dummyEPS); err != nil {
			errors = append(errors, err)
		}
	}

	if syncable, ok := g.nodePortWatcher.(serviceEndpointSliceSyncer); ok {
		if err := syncable.SyncEndpointSlicesForService(svc); err != nil {
			errors = append(errors, err)
		}
	} else if g.nodePortWatcher != nil {
		dummyEPS := &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: svc.Namespace,
				Name:      svc.Name,
				Labels: map[string]string{
					discovery.LabelServiceName: svc.Name,
				},
			},
		}
		if err := g.nodePortWatcher.AddEndpointSlice(dummyEPS); err != nil {
			errors = append(errors, err)
		}
	}
	return utilerrors.Join(errors...)
}

func (g *gateway) onEgressIPAdd(obj interface{}) {
	eip, ok := obj.(*egressipv1.EgressIP)
	if !ok {
		return
	}
	g.egressIPQueue.Add(eip.Name)
}

func (g *gateway) onEgressIPUpdate(oldObj, newObj interface{}) {
	oldEIP, ok1 := oldObj.(*egressipv1.EgressIP)
	newEIP, ok2 := newObj.(*egressipv1.EgressIP)
	if !ok1 || !ok2 {
		return
	}
	if egressIPUpdateNotNeeded(oldEIP, newEIP) {
		return
	}
	g.egressIPQueue.Add(newEIP.Name)
}

func (g *gateway) onEgressIPDelete(obj interface{}) {
	eip, ok := obj.(*egressipv1.EgressIP)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		eip, ok = tombstone.Obj.(*egressipv1.EgressIP)
		if !ok {
			return
		}
	}
	g.egressIPQueue.Add(eip.Name)
}

func (g *gateway) runEgressIPWorker(_ context.Context) {
	for g.processEgressIPItem() {
	}
}

func (g *gateway) processEgressIPItem() bool {
	name, shutdown := g.egressIPQueue.Get()
	if shutdown {
		return false
	}
	defer g.egressIPQueue.Done(name)

	err := g.reconcileEgressIP(name)
	if err != nil {
		if g.egressIPQueue.NumRequeues(name) < controller.DefaultMaxAttempts {
			klog.V(4).Infof("Error reconciling egress IP %s: %v, requeuing", name, err)
			g.egressIPQueue.AddRateLimited(name)
			return true
		}
		klog.Errorf("Dropping egress IP %s out of gateway queue after %d attempts: %v", name, controller.DefaultMaxAttempts, err)
	}
	g.egressIPQueue.Forget(name)
	return true
}

func (g *gateway) reconcileEgressIP(name string) error {
	curEIP, err := g.watchFactory.GetEgressIP(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return g.DeleteEgressIPByName(name)
		}
		return fmt.Errorf("failed to get egress IP %s: %w", name, err)
	}

	return g.EnsureEgressIP(curEIP)
}

func (g *gateway) Stop() {
	g.stopOnce.Do(g.stop)
}

func (g *gateway) stop() {
	if g.serviceQueue != nil {
		g.serviceQueue.ShutDown()
	}
	if g.endpointSliceQueue != nil {
		g.endpointSliceQueue.ShutDown()
	}
	if g.egressIPQueue != nil {
		g.egressIPQueue.ShutDown()
	}
	if g.serviceHandler != nil {
		if err := g.watchFactory.ServiceInformer().RemoveEventHandler(g.serviceHandler); err != nil {
			klog.Warningf("Failed to remove gateway Service event handler: %v", err)
		}
		g.serviceHandler = nil
	}
	if g.endpointSliceHandler != nil {
		if err := g.watchFactory.EndpointSliceInformer().RemoveEventHandler(g.endpointSliceHandler); err != nil {
			klog.Warningf("Failed to remove gateway EndpointSlice event handler: %v", err)
		}
		g.endpointSliceHandler = nil
	}
	if g.egressIPHandler != nil {
		if err := g.watchFactory.EgressIPInformer().Informer().RemoveEventHandler(g.egressIPHandler); err != nil {
			klog.Warningf("Failed to remove gateway EgressIP event handler: %v", err)
		}
		g.egressIPHandler = nil
	}
}

func (g *gateway) initHandlers() error {
	if g.watchFactory == nil {
		return nil
	}

	g.serviceQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.DefaultRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "gateway-services"},
	)
	g.endpointSliceQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.DefaultRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "gateway-endpointslices"},
	)

	var err error
	g.serviceHandler, err = g.watchFactory.ServiceInformer().AddEventHandler(
		factory.WithUpdateHandlingForObjReplace(cache.ResourceEventHandlerFuncs{
			AddFunc:    g.onServiceAdd,
			UpdateFunc: g.onServiceUpdate,
			DeleteFunc: g.onServiceDelete,
		}),
	)
	if err != nil {
		g.Stop()
		return fmt.Errorf("failed to add Service event handler: %w", err)
	}

	g.endpointSliceHandler, err = g.watchFactory.EndpointSliceInformer().AddEventHandler(
		factory.WithUpdateHandlingForObjReplace(cache.ResourceEventHandlerFuncs{
			AddFunc:    g.onEndpointSliceAdd,
			UpdateFunc: g.onEndpointSliceUpdate,
			DeleteFunc: g.onEndpointSliceDelete,
		}),
	)
	if err != nil {
		g.Stop()
		return fmt.Errorf("failed to add EndpointSlice event handler: %w", err)
	}

	if config.OVNKubernetesFeature.EnableEgressIP {
		g.egressIPQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
			controller.DefaultRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "gateway-egressips"},
		)
		g.egressIPHandler, err = g.watchFactory.EgressIPInformer().Informer().AddEventHandler(
			factory.WithUpdateHandlingForObjReplace(cache.ResourceEventHandlerFuncs{
				AddFunc:    g.onEgressIPAdd,
				UpdateFunc: g.onEgressIPUpdate,
				DeleteFunc: g.onEgressIPDelete,
			}),
		)
		if err != nil {
			g.Stop()
			return fmt.Errorf("failed to add EgressIP event handler: %w", err)
		}
	}

	return nil
}

// syncServicesOnStartup synchronizes existing services in batch during gateway Init.
func (g *gateway) syncServicesOnStartup(ctx context.Context) error {
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 60*time.Second, true, func(_ context.Context) (bool, error) {
		svcs, err := g.watchFactory.GetServices()
		if err != nil {
			klog.Errorf("Failed (will retry) to get services for initial sync: %v", err)
			return false, nil
		}
		svcObjs := make([]interface{}, len(svcs))
		for i, svc := range svcs {
			svcObjs[i] = svc
		}
		if err := g.SyncServices(svcObjs); err != nil {
			klog.Errorf("Failed (will retry) while syncing gateway services: %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed initial service sync: %w", err)
	}
	return nil
}

func (g *gateway) syncEgressIPOnStartup(ctx context.Context) error {
	var eips []*egressipv1.EgressIP
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 60*time.Second, true, func(_ context.Context) (bool, error) {
		var err error
		eips, err = g.watchFactory.GetEgressIPs()
		if err != nil {
			klog.Errorf("Failed (will retry) to get egress IPs for initial sync: %v", err)
			return false, nil
		}
		eipObjs := make([]interface{}, len(eips))
		for i, eip := range eips {
			eipObjs[i] = eip
		}
		if err := g.SyncEgressIP(eipObjs); err != nil {
			klog.Errorf("Failed (will retry) while syncing gateway egress IPs: %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed initial egress IP sync: %w", err)
	}

	return nil
}

func (g *gateway) WatchServices(ctx context.Context, workers int) error {
	if g.serviceQueue == nil {
		return fmt.Errorf("serviceQueue is not initialized")
	}

	for i := 0; i < workers; i++ {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			wait.UntilWithContext(ctx, g.runServiceWorker, time.Second)
		}()
	}

	return nil
}

func (g *gateway) WatchEndpointSlices(ctx context.Context, workers int) error {
	if g.endpointSliceQueue == nil {
		return fmt.Errorf("endpointSliceQueue is not initialized")
	}

	for i := 0; i < workers; i++ {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			wait.UntilWithContext(ctx, g.runEndpointSliceWorker, time.Second)
		}()
	}

	return nil
}

func (g *gateway) WatchEgressIPs(ctx context.Context, workers int) error {
	if g.egressIPQueue == nil {
		return fmt.Errorf("egressIPQueue is not initialized")
	}

	for i := 0; i < workers; i++ {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			wait.UntilWithContext(ctx, g.runEgressIPWorker, time.Second)
		}()
	}

	return nil
}

func (g *gateway) Init(stopChan <-chan struct{}, wg *sync.WaitGroup) error {
	g.stopChan = stopChan
	g.wg = wg

	if g.watchFactory != nil && g.serviceQueue != nil {
		initCtx := wait.ContextForChannel(stopChan)
		if err := g.syncServicesOnStartup(initCtx); err != nil {
			return err
		}
		if config.OVNKubernetesFeature.EnableEgressIP && g.egressIPQueue != nil {
			if err := g.syncEgressIPOnStartup(initCtx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *gateway) Start() error {
	if g.openflowManager != nil {
		klog.Info("Spawning Conntrack Rule Check Thread")
		err := g.openflowManager.updateBridgeFlowCache(g.nodeIPManager.ListAddresses())
		if err != nil {
			return fmt.Errorf("failed to update bridge flow cache: %w", err)
		}
		g.openflowManager.Run(g.stopChan, g.wg)
	}

	if g.nodeIPManager != nil {
		g.nodeIPManager.Run(g.stopChan, g.wg)
	}

	if g.watchFactory != nil && g.serviceQueue != nil {
		runCtx := wait.ContextForChannel(g.stopChan)
		if err := g.WatchServices(runCtx, 1); err != nil {
			return fmt.Errorf("failed to watch services: %w", err)
		}
		if err := g.WatchEndpointSlices(runCtx, 1); err != nil {
			return fmt.Errorf("failed to watch endpoint slices: %w", err)
		}
		if config.OVNKubernetesFeature.EnableEgressIP && g.egressIPQueue != nil {
			if err := g.WatchEgressIPs(runCtx, 1); err != nil {
				return fmt.Errorf("failed to watch egress IPs: %w", err)
			}
		}
	}

	return nil
}

// sets up an uplink interface for UDP Generic Receive Offload forwarding as part of
// the EnableUDPAggregation feature.
func setupUDPAggregationUplink(ifname string) error {
	e, err := ethtool.NewEthtool()
	if err != nil {
		return fmt.Errorf("failed to initialize ethtool: %v", err)
	}
	defer e.Close()

	err = e.Change(ifname, map[string]bool{
		"rx-udp-gro-forwarding": true,
	})
	if err != nil {
		return fmt.Errorf("could not enable UDP offload features on %q: %v", ifname, err)
	}

	return nil
}

func gatewayInitInternal(ovsClient libovsdbclient.Client, nodeName, gwIntf, egressGatewayIntf string, gwNextHops []net.IP, nodeSubnets, gwIPs []*net.IPNet,
	advertised bool, nodeAnnotator kube.Annotator) (
	*bridgeconfig.BridgeConfiguration, *bridgeconfig.BridgeConfiguration, error) {
	gatewayBridge, err := bridgeconfig.NewBridgeConfiguration(ovsClient, gwIntf, nodeName, types.PhysicalNetworkName, nodeSubnets, gwIPs, advertised)
	if err != nil {
		return nil, nil, fmt.Errorf("bridge for interface failed for %s: %w", gwIntf, err)
	}
	var egressGWBridge *bridgeconfig.BridgeConfiguration
	if egressGatewayIntf != "" {
		egressGWBridge, err = bridgeconfig.NewBridgeConfiguration(ovsClient, egressGatewayIntf, nodeName, types.PhysicalNetworkExGwName, nodeSubnets, nil, false)
		if err != nil {
			return nil, nil, fmt.Errorf("bridge for interface failed for %s: %w", egressGatewayIntf, err)
		}
	}

	chassisID, err := util.GetNodeChassisID()
	if err != nil {
		return nil, nil, err
	}

	// Set annotation that determines if options:gateway_mtu shall be set for this node.
	enableGatewayMTU := true
	if config.Gateway.DisablePacketMTUCheck {
		klog.Warningf("Config option disable-pkt-mtu-check is set to true. " +
			"options:gateway_mtu will be disabled on gateway routers. " +
			"IP fragmentation or large TCP/UDP payloads may not be forwarded correctly.")
		enableGatewayMTU = false
	} else {
		chkPktLengthSupported, err := util.DetectCheckPktLengthSupport(gatewayBridge.GetBridgeName())
		if err != nil {
			return nil, nil, err
		}
		if !chkPktLengthSupported {
			klog.Warningf("OVS does not support check_packet_length action. " +
				"options:gateway_mtu will be disabled on gateway routers. " +
				"IP fragmentation or large TCP/UDP payloads may not be forwarded correctly.")
			enableGatewayMTU = false
		} else {
			/* This is a work around. In order to have the most optimal performance, the packet MTU check should be
			 * disabled when OVS HW Offload is enabled on the node. The reason is that OVS HW Offload does not support
			 * packet MTU checks properly without the offload support for sFlow.
			 * The patches for sFlow in OvS: https://patchwork.ozlabs.org/project/openvswitch/list/?series=290804
			 * As of writing these offload support patches for sFlow are in review.
			 * TODO: This workaround should be removed once the offload support for sFlow patches are merged upstream OvS.
			 */
			ovsHardwareOffloadEnabled, err := util.IsOvsHwOffloadEnabled()
			if err != nil {
				return nil, nil, err
			}
			if ovsHardwareOffloadEnabled {
				klog.Warningf("OVS hardware offloading is enabled. " +
					"options:gateway_mtu will be disabled on gateway routers for performance reasons. " +
					"IP fragmentation or large TCP/UDP payloads may not be forwarded correctly.")
				enableGatewayMTU = false
			}
		}
	}
	if err := util.SetGatewayMTUSupport(nodeAnnotator, enableGatewayMTU); err != nil {
		return nil, nil, err
	}

	if config.Default.EnableUDPAggregation {
		err = setupUDPAggregationUplink(gatewayBridge.GetUplinkName())
		if err == nil && egressGWBridge != nil {
			err = setupUDPAggregationUplink(egressGWBridge.GetUplinkName())
		}
		if err != nil {
			klog.Warningf("Could not enable UDP packet aggregation on uplink interface (aggregation will be disabled): %v", err)
			config.Default.EnableUDPAggregation = false
		}
	}

	if config.IsModeDPU() || config.IsModeFull() {
		if err := util.SetStaticFDBEntry(
			gatewayBridge.GetBridgeName(),
			gatewayBridge.GetStaticFDBPort(),
			gatewayBridge.GetMAC(),
			config.Gateway.VLANID); err != nil {
			return nil, nil, err
		}
	}

	l3GwConfig := util.L3GatewayConfig{
		Mode:           config.Gateway.Mode,
		ChassisID:      chassisID,
		BridgeID:       gatewayBridge.GetBridgeName(),
		InterfaceID:    gatewayBridge.GetInterfaceID(),
		MACAddress:     gatewayBridge.GetMAC(),
		IPAddresses:    gatewayBridge.GetIPs(),
		NextHops:       gwNextHops,
		NodePortEnable: config.Gateway.NodeportEnable,
		VLANID:         &config.Gateway.VLANID,
	}
	if egressGWBridge != nil {
		l3GwConfig.EgressGWInterfaceID = egressGWBridge.GetInterfaceID()
		l3GwConfig.EgressGWMACAddress = egressGWBridge.GetMAC()
		l3GwConfig.EgressGWIPAddresses = egressGWBridge.GetIPs()
	}

	err = util.SetL3GatewayConfig(nodeAnnotator, &l3GwConfig)
	return gatewayBridge, egressGWBridge, err
}

func (g *gateway) GetGatewayBridgeIface() string {
	return g.openflowManager.getDefaultBridgeName()
}

func (g *gateway) GetGatewayIface() string {
	if config.IsModeDPU() || config.IsModeFull() {
		if g.openflowManager == nil {
			return ""
		}
		return g.openflowManager.defaultBridge.GetGatewayIface()
	} else {
		return config.Gateway.Interface
	}
}

// SetDefaultGatewayBridgeMAC updates the mac address for the OFM used to render flows with
func (g *gateway) SetDefaultGatewayBridgeMAC(macAddr net.HardwareAddr) {
	if config.IsModeDPUHost() {
		return
	}
	g.openflowManager.setDefaultBridgeMAC(macAddr)
	klog.Infof("Default gateway bridge MAC address updated to %s", macAddr)
}

func (g *gateway) SetDefaultPodNetworkAdvertised(isPodNetworkAdvertised bool) {
	if config.IsModeDPUHost() {
		return
	}
	g.openflowManager.defaultBridge.GetNetworkConfig(types.DefaultNetworkName).Advertised.Store(isPodNetworkAdvertised)
}

func (g *gateway) GetDefaultPodNetworkAdvertised() bool {
	if config.IsModeDPUHost() {
		return false
	}
	return g.openflowManager.defaultBridge.GetNetworkConfig(types.DefaultNetworkName).Advertised.Load()
}

// GetUplinkName returns the physical uplink interface name
func (g *gateway) GetUplinkName() string {
	if config.IsModeDPUHost() {
		return ""
	}
	return g.openflowManager.defaultBridge.GetUplinkName()
}

// SetDefaultBridgeGARPDropFlows will enable flows to drop GARPs if the openflow
// manager has been initialized.
func (g *gateway) SetDefaultBridgeGARPDropFlows(isDropped bool) {
	if config.IsModeDPUHost() {
		return
	}

	if g.openflowManager == nil {
		return
	}
	g.openflowManager.setDefaultBridgeGARPDrop(isDropped)
}

// Reconcile handles triggering updates to different components of a gateway, like
// OFM and Services, in response to a change with cluster-wide reach (node IP/MAC
// change, EgressIP, default-network advertised change, init). It resyncs every
// service because any service's flows may be affected.
func (g *gateway) Reconcile() error {
	return g.reconcile(g.resyncAllServices)
}

// ReconcileNetwork rebuilds the shared bridge flows after a primary UDN is added
// and resyncs only that network's namespaces' services. Advertised-state changes
// must go through UserDefinedNetworkGateway.Reconcile (reconcileAdvertisedState),
// not this.
func (g *gateway) ReconcileNetwork(netInfo util.NetInfo) error {
	return g.reconcile(func() []error {
		return g.resyncNetworkServices(netInfo)
	})
}

// ReconcileNetworkRemoval refreshes the shared gateway OpenFlow/SNAT state after a
// network's config has been removed, dropping its bridge flows. Services are not
// resynced: a UDN namespace cannot revert to the default network (the reserved label
// is immutable), so the removed network's services are either already gone (their own
// delete events clean their per-service flows) or their namespace still carries the
// label with no NAD, leaving them with no valid network to reprogram onto.
func (g *gateway) ReconcileNetworkRemoval() error {
	return g.reconcile(nil)
}

// reconcile refreshes the gateway's OpenFlow/SNAT state and, if syncServices is
// non-nil, resyncs the selected services through the gateway workqueue.
func (g *gateway) reconcile(syncServices func() []error) error {
	klog.Info("Reconciling gateway with updates")
	if config.IsModeDPU() || config.IsModeFull() {
		if g.openflowManager != nil {
			if g.nodeIPManager == nil {
				klog.V(5).Info("Skipping gateway OpenFlow reconcile because node IP manager is not initialized yet")
			} else {
				if err := g.openflowManager.updateBridgeFlowCache(g.nodeIPManager.ListAddresses()); err != nil {
					return err
				}
				// let's sync these flows immediately
				g.openflowManager.requestFlowSync()
			}
		}
	}
	// TBD updateSNATRules() gets node host-cidr by accessing gateway.nodeIPManager, which does not
	// exist in dpu-host mode.
	if config.IsModeFull() {
		err := g.updateSNATRules()
		if err != nil {
			return err
		}
	}
	// Services create OpenFlow flows as well, need to resync the affected ones.
	if g.serviceQueue != nil && syncServices != nil {
		if errs := syncServices(); errs != nil {
			return utilerrors.Join(errs...)
		}
	}
	return nil
}

// resyncAllServices enqueues every service in the cluster for reprogramming. Used
// for cluster-wide changes where any service's flows may be affected.
func (g *gateway) resyncAllServices() []error {
	svcs, err := g.watchFactory.GetServices()
	if err != nil {
		return []error{err}
	}
	return g.resyncServices(svcs)
}

// resyncNetworkServices enqueues only the services in netInfo's namespaces
// (GetNADNamespaces) for reprogramming, instead of scanning every service. A selected
// namespace whose NAD has not landed yet recovers via its own per-service workqueue retry.
func (g *gateway) resyncNetworkServices(netInfo util.NetInfo) []error {
	var svcs []*corev1.Service
	for _, namespace := range netInfo.GetNADNamespaces() {
		nsSvcs, err := g.watchFactory.GetServicesByNamespace(namespace)
		if err != nil {
			return []error{err}
		}
		svcs = append(svcs, nsSvcs...)
	}
	return g.resyncServices(svcs)
}

// resyncServices adds the given services to the workqueue.
func (g *gateway) resyncServices(svcs []*corev1.Service) []error {
	if g.serviceQueue == nil {
		return nil
	}
	var errs []error
	for _, svc := range svcs {
		key, err := cache.MetaNamespaceKeyFunc(svc)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get key for service %s/%s: %w", svc.Namespace, svc.Name, err))
			continue
		}
		klog.V(5).Infof("Adding service %s to serviceQueue", key)
		g.serviceResync.Store(key, struct{}{})
		g.serviceQueue.Add(key)
	}
	return errs
}

func (g *gateway) updateSNATRules() error {
	if config.Gateway.Mode != config.GatewayModeLocal {
		return delLocalGatewayPodSubnetNFTRules()
	}

	if g.nodeIPManager == nil {
		klog.V(5).Info("Skipping SNAT rule update because node IP manager is not initialized yet")
		return nil
	}
	if g.nodeIPManager.mgmtPort == nil {
		klog.V(5).Info("Skipping SNAT rule update because management port is not initialized yet")
		return nil
	}
	subnets := util.IPsToNetworkIPs(g.nodeIPManager.mgmtPort.GetAddresses()...)

	return addOrUpdateLocalGatewayPodSubnetNFTRules(g.GetDefaultPodNetworkAdvertised(), subnets...)
}
