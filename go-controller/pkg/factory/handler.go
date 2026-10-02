// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package factory

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	ipamclaimslister "github.com/k8snetworkplumbingwg/ipamclaims/pkg/crd/ipamclaims/v1alpha1/apis/listers/ipamclaims/v1alpha1"
	multinetworkpolicylister "github.com/k8snetworkplumbingwg/multi-networkpolicy/pkg/client/listers/k8s.cni.cncf.io/v1beta1"
	networkattachmentdefinitionlister "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	cloudprivateipconfiglister "github.com/openshift/client-go/cloudnetwork/listers/cloudnetwork/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	listers "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	netlisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	anplister "sigs.k8s.io/network-policy-api/pkg/client/listers/apis/v1alpha1"

	networkconnectlister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/clusternetworkconnect/v1/apis/listers/clusternetworkconnect/v1"
	egressfirewalllister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1/apis/listers/egressfirewall/v1"
	egressiplister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressip/v1/apis/listers/egressip/v1"
	egressqoslister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressqos/v1/apis/listers/egressqos/v1"
	egressservicelister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressservice/v1/apis/listers/egressservice/v1"
	networkqoslister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/networkqos/v1alpha1/apis/listers/networkqos/v1alpha1"
	uplinklister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/uplink/v1alpha1/apis/listers/uplink/v1alpha1"
	userdefinednetworklister "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/listers/userdefinednetwork/v1"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/metrics"
)

// Handler represents an event handler and is private to the factory module
type Handler struct {
	base cache.FilteringResourceEventHandler

	id uint64
	// tombstone is used to track the handler's lifetime. handlerAlive
	// indicates the handler can be called, while handlerDead indicates
	// it has been scheduled for removal and should not be called.
	// tombstone should only be set using atomic operations since it is
	// used from multiple goroutines.
	tombstone uint32

	// oType is the type of object this handler is registered for; used to
	// resolve tombstones on delete and to label metrics.
	oType reflect.Type

	registration cache.ResourceEventHandlerRegistration
}

func (h *Handler) OnAdd(obj interface{}, isInInitialList bool) {
	if atomic.LoadUint32(&h.tombstone) == handlerDead {
		return
	}
	name := h.oType.Elem().Name()
	metrics.MetricResourceUpdateCount.WithLabelValues(name, "add").Inc()
	start := time.Now()
	h.base.OnAdd(obj, isInInitialList)
	metrics.MetricResourceAddLatency.Observe(time.Since(start).Seconds())
}

func (h *Handler) OnUpdate(oldObj, newObj interface{}) {
	if atomic.LoadUint32(&h.tombstone) == handlerDead {
		return
	}
	name := h.oType.Elem().Name()
	metrics.MetricResourceUpdateCount.WithLabelValues(name, "update").Inc()
	start := time.Now()
	old := oldObj.(metav1.Object)
	new := newObj.(metav1.Object)
	if old.GetUID() != new.GetUID() {
		// This occurs not so often, so log this occurrence.
		klog.Infof("Object %s/%s is replaced, invoking delete followed by add handler", new.GetNamespace(), new.GetName())
		h.base.OnDelete(oldObj)
		h.base.OnAdd(newObj, false)
	} else {
		h.base.OnUpdate(oldObj, newObj)
	}
	metrics.MetricResourceUpdateLatency.Observe(time.Since(start).Seconds())
}

func (h *Handler) OnDelete(obj interface{}) {
	if atomic.LoadUint32(&h.tombstone) == handlerDead {
		return
	}
	realObj, err := ensureObjectOnDelete(obj, h.oType)
	if err != nil {
		klog.Errorf("Error in DeleteFunc: %v", err)
		return
	}
	name := h.oType.Elem().Name()
	metrics.MetricResourceUpdateCount.WithLabelValues(name, "delete").Inc()
	start := time.Now()
	h.base.OnDelete(realObj)
	metrics.MetricResourceDeleteLatency.Observe(time.Since(start).Seconds())
}

func (h *Handler) FilterFunc(obj interface{}) bool {
	return h.base.FilterFunc(obj)
}

func (h *Handler) kill() bool {
	return atomic.CompareAndSwapUint32(&h.tombstone, handlerAlive, handlerDead)
}

func ensureObjectOnDelete(obj interface{}, expectedType reflect.Type) (interface{}, error) {
	if expectedType == reflect.TypeOf(obj) {
		return obj, nil
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, fmt.Errorf("couldn't get object from tombstone: %+v", obj)
	}
	obj = tombstone.Obj
	objType := reflect.TypeOf(obj)
	if expectedType != objType {
		return nil, fmt.Errorf("expected tombstone object resource type %v but got %v", expectedType, objType)
	}
	return obj, nil
}

type listerInterface interface{}

type informer struct {
	sync.Mutex
	oType    reflect.Type
	inf      cache.SharedIndexInformer
	lister   listerInterface
	handlers map[uint64]*Handler
}

func (i *informer) addHandler(id uint64, filterFunc func(obj interface{}) bool, funcs cache.ResourceEventHandler) (*Handler, error) {
	handler := &Handler{
		base: cache.FilteringResourceEventHandler{
			FilterFunc: filterFunc,
			Handler:    funcs,
		},
		id:        id,
		tombstone: handlerAlive,
		oType:     i.oType,
	}

	reg, err := i.inf.AddEventHandler(handler)
	if err != nil {
		return nil, err
	}
	handler.registration = reg

	i.Lock()
	i.handlers[id] = handler
	i.Unlock()

	return handler, nil
}

func (i *informer) removeHandler(handler *Handler) error {
	if !handler.kill() {
		klog.Errorf("Removing already-removed %v event handler %d", i.oType, handler.id)
		return nil
	}

	klog.V(5).Infof("Sending %v event handler %d for removal", i.oType, handler.id)

	i.Lock()
	delete(i.handlers, handler.id)
	i.Unlock()

	if handler.registration != nil {
		if err := i.inf.RemoveEventHandler(handler.registration); err != nil {
			return err
		}
	}
	klog.V(5).Infof("Removed %v event handler %d", i.oType, handler.id)
	return nil
}

func (i *informer) shutdown() {
	i.Lock()
	handlers := make([]*Handler, 0, len(i.handlers))
	for _, h := range i.handlers {
		handlers = append(handlers, h)
	}
	i.Unlock()

	for _, h := range handlers {
		_ = i.removeHandler(h)
	}
}

func newInformerLister(oType reflect.Type, sharedInformer cache.SharedIndexInformer) (listerInterface, error) {
	switch oType {
	case PodType:
		return listers.NewPodLister(sharedInformer.GetIndexer()), nil
	case ServiceType:
		return listers.NewServiceLister(sharedInformer.GetIndexer()), nil
	case NamespaceType:
		return listers.NewNamespaceLister(sharedInformer.GetIndexer()), nil
	case NodeType:
		return listers.NewNodeLister(sharedInformer.GetIndexer()), nil
	case PolicyType:
		return netlisters.NewNetworkPolicyLister(sharedInformer.GetIndexer()), nil
	case EgressFirewallType:
		return egressfirewalllister.NewEgressFirewallLister(sharedInformer.GetIndexer()), nil
	case AdminNetworkPolicyType:
		return anplister.NewAdminNetworkPolicyLister(sharedInformer.GetIndexer()), nil
	case BaselineAdminNetworkPolicyType:
		return anplister.NewBaselineAdminNetworkPolicyLister(sharedInformer.GetIndexer()), nil
	case EgressIPType:
		return egressiplister.NewEgressIPLister(sharedInformer.GetIndexer()), nil
	case CloudPrivateIPConfigType:
		return cloudprivateipconfiglister.NewCloudPrivateIPConfigLister(sharedInformer.GetIndexer()), nil
	case EndpointSliceType:
		return discoverylisters.NewEndpointSliceLister(sharedInformer.GetIndexer()), nil
	case EgressQoSType:
		return egressqoslister.NewEgressQoSLister(sharedInformer.GetIndexer()), nil
	case NetworkAttachmentDefinitionType:
		return networkattachmentdefinitionlister.NewNetworkAttachmentDefinitionLister(sharedInformer.GetIndexer()), nil
	case MultiNetworkPolicyType:
		return multinetworkpolicylister.NewMultiNetworkPolicyLister(sharedInformer.GetIndexer()), nil
	case EgressServiceType:
		return egressservicelister.NewEgressServiceLister(sharedInformer.GetIndexer()), nil
	case IPAMClaimsType:
		return ipamclaimslister.NewIPAMClaimLister(sharedInformer.GetIndexer()), nil
	case UserDefinedNetworkType:
		return userdefinednetworklister.NewUserDefinedNetworkLister(sharedInformer.GetIndexer()), nil
	case ClusterUserDefinedNetworkType:
		return userdefinednetworklister.NewClusterUserDefinedNetworkLister(sharedInformer.GetIndexer()), nil
	case UplinkType:
		return uplinklister.NewUplinkLister(sharedInformer.GetIndexer()), nil
	case UplinkStateType:
		return uplinklister.NewUplinkStateLister(sharedInformer.GetIndexer()), nil
	case ClusterNetworkConnectType:
		return networkconnectlister.NewClusterNetworkConnectLister(sharedInformer.GetIndexer()), nil
	case NetworkQoSType:
		return networkqoslister.NewNetworkQoSLister(sharedInformer.GetIndexer()), nil
	}

	return nil, fmt.Errorf("cannot create lister from type %v", oType)
}

func newInformer(oType reflect.Type, sharedInformer cache.SharedIndexInformer) (*informer, error) {
	lister, err := newInformerLister(oType, sharedInformer)
	if err != nil {
		return nil, err
	}

	return &informer{
		oType:    oType,
		inf:      sharedInformer,
		lister:   lister,
		handlers: make(map[uint64]*Handler),
	}, nil
}
