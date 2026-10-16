// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

type retryReviewReader struct {
	client.Reader
	key   client.ObjectKey
	err   error
	fired bool
	keys  []client.ObjectKey
}

func (r *retryReviewReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.keys = append(r.keys, key)
	if !r.fired && key == r.key {
		r.fired = true
		return r.err
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestRetryCleanupDependencyReadFailureStopsLaunchAndResumes(t *testing.T) {
	ctx := context.Background()
	workflow := &nvcrev1alpha1.Workflow{
		Name: "wf", Namespace: "ns", UID: "retry-reader-workflow-uid",
		Spec: nvcrev1alpha1.WorkflowSpec{
			Dependencies: []nvcrev1alpha1.DependencySpec{{Raw: []byte(
				`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"retry-reader-dependency"}}`)}},
			JobTemplate: nvcrev1alpha1.JobTemplateSpec{Spec: nvcrev1alpha1.JobSpec{
				Workload: nvcrev1alpha1.WorkloadSpec{TrainJob: &trainerv1alpha1.TrainJobSpec{
					RuntimeRef: trainerv1alpha1.RuntimeRef{Name: "retry-reader-dependency"},
				}},
			}},
			Orchestration: nvcrev1alpha1.OrchestrationSpec{
				Iterations: 2,
				Execution:  nvcrev1alpha1.ExecutionSpec{RetryFailedGroups: 1},
			},
		},
		Status: nvcrev1alpha1.WorkflowStatus{Orchestration: &nvcrev1alpha1.OrchestrationStatus{
			CurrentIteration: 1, TotalGroups: 1, NodesPerJob: 1,
			Groups: []nvcrev1alpha1.GroupStatus{{Name: "a", Phase: nvcrev1alpha1.GroupPending, Retries: 1}},
		}},
	}
	r := &WorkflowReconciler{}
	oldJobName := r.getGroupJobName(workflow, "a", 1, 0)
	group := &workflow.Status.Orchestration.Groups[0]
	prepared, err := prepareJobDependencies(workflow, group, workflow.Status.Orchestration, groupJobSpec(workflow))
	require.NoError(t, err)
	require.Len(t, prepared.renamed, 1)
	dep := &unstructured.Unstructured{}
	require.NoError(t, json.Unmarshal(prepared.renamed[0].Raw, &dep.Object))
	dep.SetLabels(map[string]string{labelWorkflowTracking: workflow.Name})
	dep.SetAnnotations(map[string]string{
		annotationWorkflowUID:  string(workflow.UID),
		annotationGroupAttempt: "0",
	})
	depName := dep.GetName()
	workflow.Status.DependencyRefs = []nvcrev1alpha1.DependencyResourceRef{{
		APIVersion: dep.GetAPIVersion(), Kind: dep.GetKind(), Name: depName, Namespace: workflow.Namespace,
		Scope: scopeJob, GroupName: group.Name, Iteration: 1,
	}}

	job := &nvcrev1alpha1.Job{Name: oldJobName, Namespace: workflow.Namespace, UID: "old-job"}
	job.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(workflow, nvcrev1alpha1.GroupVersion.WithKind("Workflow"))}
	scheme := newFallbackScheme(t)
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workflow, job, dep).
		WithStatusSubresource(&nvcrev1alpha1.Workflow{}, &nvcrev1alpha1.Job{}).Build()
	readErr := errors.New("transient live API read failure")
	reader := &retryReviewReader{Reader: c, key: client.ObjectKey{Namespace: workflow.Namespace, Name: depName}, err: readErr}
	r.Client, r.APIReader, r.Scheme = c, reader, scheme

	_, err = r.launchPendingGroups(ctx, workflow, workflow.Status.Orchestration)
	require.NoError(t, err, "the first pass requests deletion of the old Job and waits for it to disappear")
	_, err = r.launchPendingGroups(ctx, workflow, workflow.Status.Orchestration)
	require.True(t, reader.fired, "live reads: %v", reader.keys)
	require.ErrorIs(t, err, readErr)
	stored := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(workflow), stored))
	require.Equal(t, 1, stored.Status.Orchestration.Groups[0].Retries)
	require.Equal(t, nvcrev1alpha1.GroupPending, stored.Status.Orchestration.Groups[0].Phase)
	gotDep := &unstructured.Unstructured{}
	gotDep.SetAPIVersion(dep.GetAPIVersion())
	gotDep.SetKind(dep.GetKind())
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dep), gotDep))
	currentJobName := r.getGroupJobName(workflow, "a", 1, 1)
	require.Error(t, c.Get(ctx, client.ObjectKey{Namespace: workflow.Namespace, Name: currentJobName}, &nvcrev1alpha1.Job{}))

	for range 4 {
		fresh := &nvcrev1alpha1.Workflow{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(workflow), fresh))
		_, err = r.launchPendingGroups(ctx, fresh, fresh.Status.Orchestration)
		require.NoError(t, err)
	}
	fresh := &nvcrev1alpha1.Workflow{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(workflow), fresh))
	require.Equal(t, 1, fresh.Status.Orchestration.Groups[0].Retries)
	require.Equal(t, nvcrev1alpha1.GroupRunning, fresh.Status.Orchestration.Groups[0].Phase)
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: workflow.Namespace, Name: currentJobName}, &nvcrev1alpha1.Job{}))
}
