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

func TestWorkflowProgressConflictDoesNotOverwriteConcurrentFailure(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, nvcrev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	const (
		workflowName = "workflow-failure-observation"
		jobName      = "workflow-failure-observation-job"
	)
	wf := &nvcrev1alpha1.Workflow{
		Name: workflowName, Namespace: testNS, Generation: 1,
		Spec: nvcrev1alpha1.WorkflowSpec{Orchestration: nvcrev1alpha1.OrchestrationSpec{
			Placement: nvcrev1alpha1.PlacementUnpinned,
		}},
		Status: nvcrev1alpha1.WorkflowStatus{Conditions: []metav1.Condition{
			{Type: nvcrev1alpha1.WorkflowInProgress, Status: metav1.ConditionTrue, Reason: "JobRunning"},
			{Type: nvcrev1alpha1.WorkflowSucceeded, Status: metav1.ConditionFalse, Reason: ReasonNotApplicable},
			{Type: nvcrev1alpha1.WorkflowFailed, Status: metav1.ConditionFalse, Reason: ReasonNotApplicable},
		}, Orchestration: &nvcrev1alpha1.OrchestrationStatus{
			CurrentIteration: 1, TotalGroups: 1, NodesPerJob: 1,
			Groups: []nvcrev1alpha1.GroupStatus{{Name: "group-a", Phase: nvcrev1alpha1.GroupRunning,
				Nodes: []string{}, JobRef: &nvcrev1alpha1.WorkloadReference{
					APIVersion: nvcrev1alpha1.GroupVersion.String(), Kind: kindJob,
					Name: jobName, Namespace: testNS,
				}}},
		}},
	}
	job := &nvcrev1alpha1.Job{Name: jobName, Namespace: testNS}
	pod := &corev1.Pod{
		Name: "placed", Namespace: testNS, Labels: map[string]string{nodemonitor.NVCREJobLabel: jobName},
		Spec: corev1.PodSpec{NodeName: testNodeA},
	}

	firstWrite := true
	conditionTypes := []string{
		nvcrev1alpha1.WorkflowInProgress,
		nvcrev1alpha1.WorkflowSucceeded,
		nvcrev1alpha1.WorkflowFailed,
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(wf, job, pod).
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
				applyExclusiveConditions(&live.Status.Conditions, conditionTypes,
					nvcrev1alpha1.WorkflowFailed, "ConcurrentFailure", "terminal failure", live.Generation)
				require.NoError(t, c.Status().Update(ctx, live))
				return apierrors.NewConflict(
					schema.GroupResource{Group: nvcrev1alpha1.GroupVersion.Group, Resource: workflowResourceName},
					obj.GetName(), errors.New("concurrent terminal failure"))
			}
			return c.Status().Update(ctx, obj, opts...)
		}}).Build()

	r := &WorkflowReconciler{Client: c, Scheme: scheme, NodeDiscoverer: nodemonitor.NewNodeDiscoverer(c)}
	stale := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(wf), stale))
	_, err := r.updateStatusFromJobs(ctx, stale)
	require.NoError(t, err)

	stored := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(wf), stored))
	require.True(t, CondIsTrue(stored.Status.Conditions, nvcrev1alpha1.WorkflowFailed))
	require.False(t, CondIsTrue(stored.Status.Conditions, nvcrev1alpha1.WorkflowInProgress))
}
