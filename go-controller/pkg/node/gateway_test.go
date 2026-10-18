// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	egressipv1 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	egressipfake "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/egressip/v1/apis/clientset/versioned/fake"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

type fakeServiceAndEndpointsEventHandler struct {
	lock            sync.Mutex
	serviceErr      error
	addedServices   []*corev1.Service
	updatedServices [][2]*corev1.Service
	deletedServices []*corev1.Service
	addedEPS        []*discovery.EndpointSlice
	updatedEPS      [][2]*discovery.EndpointSlice
	deletedEPS      []*discovery.EndpointSlice
}

func (f *fakeServiceAndEndpointsEventHandler) AddService(svc *corev1.Service) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.serviceErr != nil {
		return f.serviceErr
	}
	f.addedServices = append(f.addedServices, svc)
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) UpdateService(old, new *corev1.Service) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.serviceErr != nil {
		return f.serviceErr
	}
	if old == nil {
		for i, s := range f.addedServices {
			if s.Name == new.Name && s.Namespace == new.Namespace {
				old = s
				f.addedServices = append(f.addedServices[:i], f.addedServices[i+1:]...)
				break
			}
		}
	}
	if old == nil {
		f.addedServices = append(f.addedServices, new)
		return nil
	}
	f.updatedServices = append(f.updatedServices, [2]*corev1.Service{old, new})
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) DeleteService(svc *corev1.Service) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.deletedServices = append(f.deletedServices, svc)
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) SyncServices([]interface{}) error { return nil }
func (f *fakeServiceAndEndpointsEventHandler) SyncEndpointSlicesForService(svc *corev1.Service) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.serviceErr != nil {
		return f.serviceErr
	}
	dummyEPS := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: svc.Namespace,
			Name:      svc.Name,
			Labels: map[string]string{
				discovery.LabelServiceName: svc.Name,
			},
		},
	}
	f.addedEPS = append(f.addedEPS, dummyEPS)
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) AddEndpointSlice(epSlice *discovery.EndpointSlice) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.addedEPS = append(f.addedEPS, epSlice)
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) UpdateEndpointSlice(old, new *discovery.EndpointSlice) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.updatedEPS = append(f.updatedEPS, [2]*discovery.EndpointSlice{old, new})
	return nil
}
func (f *fakeServiceAndEndpointsEventHandler) DeleteEndpointSlice(epSlice *discovery.EndpointSlice) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.deletedEPS = append(f.deletedEPS, epSlice)
	return nil
}

var _ = Describe("Gateway Reconciliation", func() {
	var (
		fakeWatcher *fakeServiceAndEndpointsEventHandler
		fakeClient  *util.OVNNodeClientset
		wf          *factory.WatchFactory
		gw          *gateway
	)

	BeforeEach(func() {
		Expect(config.PrepareTestConfig()).To(Succeed())
		config.OVNKubernetesFeature.EnableEgressIP = true

		fakeWatcher = &fakeServiceAndEndpointsEventHandler{}
		fakeClient = &util.OVNNodeClientset{
			KubeClient:     fake.NewSimpleClientset(),
			EgressIPClient: egressipfake.NewSimpleClientset(),
		}
	})

	JustBeforeEach(func() {
		var err error
		wf, err = factory.NewNodeWatchFactory(fakeClient, "node")
		Expect(err).NotTo(HaveOccurred())
		Expect(wf.Start()).To(Succeed())

		gw = &gateway{nodePortWatcher: fakeWatcher, watchFactory: wf}
		Expect(gw.initHandlers()).To(Succeed())
	})

	JustAfterEach(func() {
		gw.Stop()
		wf.Shutdown()
		_ = config.PrepareTestConfig()
	})

	Context("Service reconciliation", func() {
		It("onServiceAdd enqueues the service key", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			defer q.ShutDown()
			gw.serviceQueue = q

			gw.onServiceAdd(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"}})
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("default/my-service"))
		})

		It("onServiceUpdate ignores service when networking fields are unchanged", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			defer q.ShutDown()
			gw.serviceQueue = q

			oldSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service", Annotations: map[string]string{"a": "b"}}}
			newSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service", Annotations: map[string]string{"a": "c"}}}
			gw.onServiceUpdate(oldSvc, newSvc)
			Expect(q.Len()).To(Equal(0))
		})

		It("onServiceUpdate enqueues service when networking fields change", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			defer q.ShutDown()
			gw.serviceQueue = q

			oldSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.1", Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080}}}}
			newSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.1", Ports: []corev1.ServicePort{{Port: 80, NodePort: 30081}}}}
			gw.onServiceUpdate(oldSvc, newSvc)
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("default/my-service"))
		})

		It("onServiceUpdate enqueues service when HealthCheckNodePort changes", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			defer q.ShutDown()
			gw.serviceQueue = q

			oldSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"},
				Spec: corev1.ServiceSpec{
					HealthCheckNodePort: 30000,
				},
			}
			newSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"},
				Spec: corev1.ServiceSpec{
					HealthCheckNodePort: 30001,
				},
			}
			gw.onServiceUpdate(oldSvc, newSvc)
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("default/my-service"))
		})

		Context("with pre-created service", func() {
			var svc *corev1.Service
			BeforeEach(func() {
				svc = &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80, NodePort: 30081}},
					},
				}
				_, err := fakeClient.KubeClient.CoreV1().Services("default").Create(context.TODO(), svc, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			})

			It("reconcileService adds service when not yet configured", func() {
				Expect(gw.reconcileService("default/my-service")).To(Succeed())
				Expect(fakeWatcher.addedServices).To(HaveLen(1))
				Expect(fakeWatcher.addedServices[0].Name).To(Equal("my-service"))
			})

			It("reconcileService updates service when changed", func() {
				oldSvc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080}},
					},
				}
				fakeWatcher.addedServices = []*corev1.Service{oldSvc}

				Expect(gw.reconcileService("default/my-service")).To(Succeed())
				Expect(fakeWatcher.updatedServices).To(HaveLen(1))
				Expect(fakeWatcher.updatedServices[0][0]).To(Equal(oldSvc))
				Expect(fakeWatcher.updatedServices[0][1].Spec.Ports[0].NodePort).To(Equal(int32(30081)))
			})

			It("reconcileService force adds service when resync is marked", func() {
				gw.serviceResync.Store("default/my-service", struct{}{})

				Expect(gw.reconcileService("default/my-service")).To(Succeed())
				Expect(fakeWatcher.addedServices).To(HaveLen(1))
				Expect(fakeWatcher.addedServices[0].Name).To(Equal("my-service"))
			})

			It("processServiceItem drops item after max attempts exceeded", func() {
				fakeWatcher.serviceErr = fmt.Errorf("synthetic error")
				key := "default/my-service"
				for i := 0; i < controller.DefaultMaxAttempts; i++ {
					gw.serviceQueue.Add(key)
					Expect(gw.processServiceItem()).To(BeTrue())
					Expect(gw.serviceQueue.NumRequeues(key)).To(Equal(i + 1))
				}

				// Final attempt - exceeds max attempts, dropped from queue
				gw.serviceQueue.Add(key)
				Expect(gw.processServiceItem()).To(BeTrue())
				Expect(gw.serviceQueue.NumRequeues(key)).To(Equal(0))
			})
		})

		It("reconcileService deletes service when not found in informer", func() {
			Expect(gw.reconcileService("default/my-service")).To(Succeed())
			Expect(fakeWatcher.deletedServices).To(HaveLen(1))
			Expect(fakeWatcher.deletedServices[0].Name).To(Equal("my-service"))
		})

		It("resyncServices marks resync and enqueues services for forced reprogram", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			defer q.ShutDown()
			gw.serviceQueue = q

			svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"}}

			Expect(gw.resyncServices([]*corev1.Service{svc})).To(BeEmpty())
			Expect(q.Len()).To(Equal(1))
			_, resyncMarked := gw.serviceResync.Load("default/my-service")
			Expect(resyncMarked).To(BeTrue())

			By("reconcileService calls DeleteService when service was deleted even after resync marked")
			Expect(gw.reconcileService("default/my-service")).To(Succeed())
			Expect(fakeWatcher.deletedServices).To(HaveLen(1))
			Expect(fakeWatcher.deletedServices[0].Name).To(Equal("my-service"))
			_, resyncMarked = gw.serviceResync.Load("default/my-service")
			Expect(resyncMarked).To(BeFalse())
		})
	})

	Context("EndpointSlice reconciliation", func() {
		It("onEndpointSliceAdd enqueues the service key", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "epq"})
			defer q.ShutDown()
			gw.endpointSliceQueue = q

			gw.onEndpointSliceAdd(&discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "my-eps",
					Labels: map[string]string{
						discovery.LabelServiceName: "my-service",
					},
				},
			})
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("default/my-service"))
		})

		It("onEndpointSliceUpdate enqueues both old and new services when service label changes", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "epq"})
			defer q.ShutDown()
			gw.endpointSliceQueue = q

			oldEPS := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "my-eps",
					Labels: map[string]string{
						discovery.LabelServiceName: "old-service",
					},
				},
			}
			newEPS := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "my-eps",
					Labels: map[string]string{
						discovery.LabelServiceName: "new-service",
					},
				},
			}
			gw.onEndpointSliceUpdate(oldEPS, newEPS)
			Expect(q.Len()).To(Equal(2))
			item1, _ := q.Get()
			item2, _ := q.Get()
			Expect([]string{item1, item2}).To(ConsistOf("default/old-service", "default/new-service"))
		})

		Context("with pre-created endpoint slice and service", func() {
			BeforeEach(func() {
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-service"},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80, NodePort: 30081}},
					},
				}
				_, err := fakeClient.KubeClient.CoreV1().Services("default").Create(context.TODO(), svc, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())

				eps := &discovery.EndpointSlice{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "my-eps",
						Labels: map[string]string{
							discovery.LabelServiceName: "my-service",
						},
					},
					Ports: []discovery.EndpointPort{
						{Port: ptr.To(int32(8081))},
					},
				}
				_, err = fakeClient.KubeClient.DiscoveryV1().EndpointSlices("default").Create(context.TODO(), eps, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			})

			It("reconcileEndpointSlice syncs endpoints for service", func() {
				Expect(gw.reconcileEndpointSlice("default/my-service")).To(Succeed())
				Expect(fakeWatcher.addedEPS).To(HaveLen(1))
				Expect(fakeWatcher.addedEPS[0].Name).To(Equal("my-service"))
			})
		})

		It("reconcileEndpointSlice no-ops when service is not found in informer", func() {
			Expect(gw.reconcileEndpointSlice("default/non-existent")).To(Succeed())
			Expect(fakeWatcher.deletedEPS).To(BeEmpty())
		})
	})

	Context("EgressIP reconciliation", func() {
		It("onEgressIPAdd enqueues the egress IP name", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "eipq"})
			defer q.ShutDown()
			gw.egressIPQueue = q

			gw.onEgressIPAdd(&egressipv1.EgressIP{ObjectMeta: metav1.ObjectMeta{Name: "eip-1"}})
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("eip-1"))
		})

		It("onEgressIPUpdate ignores egress IP when spec and status are unchanged", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "eipq"})
			defer q.ShutDown()
			gw.egressIPQueue = q

			oldEIP := &egressipv1.EgressIP{ObjectMeta: metav1.ObjectMeta{Name: "eip-1", UID: "uid-1"}}
			newEIP := &egressipv1.EgressIP{ObjectMeta: metav1.ObjectMeta{Name: "eip-1", UID: "uid-1"}}
			gw.onEgressIPUpdate(oldEIP, newEIP)
			Expect(q.Len()).To(Equal(0))
		})

		It("onEgressIPUpdate enqueues egress IP when mark annotation changes", func() {
			q := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "eipq"})
			defer q.ShutDown()
			gw.egressIPQueue = q

			oldEIP := &egressipv1.EgressIP{
				ObjectMeta: metav1.ObjectMeta{
					Name: "eip-1",
					Annotations: map[string]string{
						util.EgressIPMarkAnnotation: "100",
					},
				},
			}
			newEIP := &egressipv1.EgressIP{
				ObjectMeta: metav1.ObjectMeta{
					Name: "eip-1",
					Annotations: map[string]string{
						util.EgressIPMarkAnnotation: "200",
					},
				},
			}
			gw.onEgressIPUpdate(oldEIP, newEIP)
			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item).To(Equal("eip-1"))
		})

		It("reconcileEgressIP reconciles egress IP when created in informer", func() {
			curEIP := &egressipv1.EgressIP{ObjectMeta: metav1.ObjectMeta{Name: "eip-1", UID: "new-uid"}}
			_, err := fakeClient.EgressIPClient.K8sV1().EgressIPs().Create(context.TODO(), curEIP, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				_, err := gw.watchFactory.GetEgressIP("eip-1")
				return err
			}, 5*time.Second).Should(Succeed())

			Expect(gw.reconcileEgressIP("eip-1")).To(Succeed())
		})

		It("reconcileEgressIP succeeds when not found in informer", func() {
			Expect(gw.reconcileEgressIP("eip-1")).To(Succeed())
		})
	})

	Context("Lifecycle and Stop", func() {
		It("WatchServices, WatchEndpointSlices, and WatchEgressIPs start workers", func() {
			stopChan := make(chan struct{})
			wg := &sync.WaitGroup{}
			defer func() {
				gw.Stop()
				close(stopChan)
				wg.Wait()
			}()

			Expect(gw.Init(stopChan, wg)).To(Succeed())
			runCtx := wait.ContextForChannel(stopChan)

			Expect(gw.WatchServices(runCtx, 1)).To(Succeed())
			Expect(gw.WatchEndpointSlices(runCtx, 1)).To(Succeed())
			Expect(gw.WatchEgressIPs(runCtx, 1)).To(Succeed())
		})

		It("Start initializes workers and syncs state", func() {
			stopChan := make(chan struct{})
			wg := &sync.WaitGroup{}
			defer func() {
				gw.Stop()
				close(stopChan)
				wg.Wait()
			}()

			Expect(gw.Init(stopChan, wg)).To(Succeed())
			Expect(gw.Start()).To(Succeed())
		})

		It("WatchServices processes pre-existing services and calls AddService", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "init-service"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080}},
				},
			}
			_, err := fakeClient.KubeClient.CoreV1().Services("default").Create(context.TODO(), svc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			stopChan := make(chan struct{})
			wg := &sync.WaitGroup{}
			defer func() {
				gw.Stop()
				close(stopChan)
				wg.Wait()
			}()

			Expect(gw.Init(stopChan, wg)).To(Succeed())
			runCtx := wait.ContextForChannel(stopChan)
			Expect(gw.WatchServices(runCtx, 1)).To(Succeed())

			Eventually(func() []*corev1.Service {
				fakeWatcher.lock.Lock()
				defer fakeWatcher.lock.Unlock()
				return fakeWatcher.addedServices
			}, 5*time.Second).Should(HaveLen(1))
		})

		It("Stop shuts down all queues and is idempotent", func() {
			sq := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "sq"})
			epq := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "epq"})
			eipq := workqueue.NewTypedRateLimitingQueueWithConfig(controller.DefaultRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "eipq"})

			gw.serviceQueue = sq
			gw.endpointSliceQueue = epq
			gw.egressIPQueue = eipq

			gw.Stop()
			Expect(sq.ShuttingDown()).To(BeTrue())
			Expect(epq.ShuttingDown()).To(BeTrue())
			Expect(eipq.ShuttingDown()).To(BeTrue())
			Expect(func() { gw.Stop() }).NotTo(Panic())
		})
	})
})
