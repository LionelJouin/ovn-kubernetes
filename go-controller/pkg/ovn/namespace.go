// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func isNamespaceMulticastEnabled(annotations map[string]string) bool {
	return annotations[util.NsMulticastAnnotation] == "true"
}
