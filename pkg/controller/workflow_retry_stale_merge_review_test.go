// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/nodemonitor"
)

func TestStaleJobObservationDoesNotRestoreRunningGroupAfterRetry(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, nvcrev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	const jobName = "workflow-stale-observation-job"
	wf := &nvcrev1alpha1.Workflow{
		Name: "wf", Namespace: "ns", Generation: 1,
		Spec: nvcrev1alpha1.WorkflowSpec{Orchestration: nvcrev1alpha1.OrchestrationSpec{
			Placement: nvcrev1alpha1.PlacementUnpinned,
		}},
		Status: nvcrev1alpha1.WorkflowStatus{
			Conditions: []metav1.Condition{{Type: nvcrev1alpha1.WorkflowInProgress,
				Status: metav1.ConditionTrue, Reason: ReasonJobRunning}},
			Orchestration: &nvcrev1alpha1.OrchestrationStatus{
				CurrentIteration: 1, TotalGroups: 1, NodesPerJob: 1,
				Groups: []nvcrev1alpha1.GroupStatus{{
					Name: "group-a", Phase: nvcrev1alpha1.GroupRunning, Nodes: []string{},
					JobRef: &nvcrev1alpha1.WorkloadReference{
						APIVersion: nvcrev1alpha1.GroupVersion.String(), Kind: kindJob,
						Name: jobName, Namespace: "ns",
					},
				}},
			},
		},
	}
	job := &nvcrev1alpha1.Job{Name: jobName, Namespace: "ns"}
	pod := &corev1.Pod{
		Name: "placed", Namespace: "ns", Labels: map[string]string{nodemonitor.NVCREJobLabel: jobName},
		Spec: corev1.PodSpec{NodeName: testNodeA},
	}
	firstWrite := true
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wf, job, pod).
		WithStatusSubresource(&nvcrev1alpha1.Workflow{}, &nvcrev1alpha1.Job{}).
		WithIndex(&corev1.Pod{}, nodemonitor.PodNVCREJobIndexField, func(obj client.Object) []string {
			p, ok := obj.(*corev1.Pod)
			if !ok || p.Labels[nodemonitor.NVCREJobLabel] == "" {
				return nil
			}
			return []string{p.Labels[nodemonitor.NVCREJobLabel]}
		}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(
			ctx context.Context, c client.Client, _ string, obj client.Object,
			opts ...client.SubResourceUpdateOption,
		) error {
			if firstWrite {
				firstWrite = false
				live := &nvcrev1alpha1.Workflow{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), live))
				g := &live.Status.Orchestration.Groups[0]
				g.Phase = nvcrev1alpha1.GroupPending
				g.Retries = 1
				g.JobRef = nil
				require.NoError(t, c.Status().Update(ctx, live))
				return apierrors.NewConflict(
					schema.GroupResource{Group: nvcrev1alpha1.GroupVersion.Group, Resource: workflowResourceName},
					obj.GetName(), errors.New("retry transition won the status race"))
			}
			return c.Status().Update(ctx, obj, opts...)
		}}).Build()

	cached := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(wf), cached))
	r := &WorkflowReconciler{Client: c, Scheme: scheme, NodeDiscoverer: nodemonitor.NewNodeDiscoverer(c)}
	_, err := r.updateStatusFromJobs(ctx, cached)
	require.NoError(t, err)

	stored := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(wf), stored))
	got := stored.Status.Orchestration.Groups[0]
	require.Equal(t, nvcrev1alpha1.GroupPending, got.Phase)
	require.Equal(t, 1, got.Retries)
	require.Nil(t, got.JobRef)
}
