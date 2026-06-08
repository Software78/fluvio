package fluvio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type wfArgs struct{ id string }

func (a wfArgs) Kind() string { return "wf-" + a.id }

func TestValidateWorkflowDAG(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		tasks := []workflowTaskDef{
			{taskID: "a", dependsOn: []string{"b"}, args: wfArgs{id: "a"}},
			{taskID: "b", dependsOn: []string{"a"}, args: wfArgs{id: "b"}},
		}
		require.ErrorIs(t, validateWorkflowDAG(tasks), ErrWorkflowCycle)
	})

	t.Run("unknown dependency", func(t *testing.T) {
		tasks := []workflowTaskDef{
			{taskID: "a", dependsOn: []string{"missing"}, args: wfArgs{id: "a"}},
		}
		require.ErrorIs(t, validateWorkflowDAG(tasks), ErrInvalidWorkflow)
	})

	t.Run("duplicate task id", func(t *testing.T) {
		tasks := []workflowTaskDef{
			{taskID: "a", args: wfArgs{id: "a"}},
			{taskID: "a", args: wfArgs{id: "a"}},
		}
		require.ErrorIs(t, validateWorkflowDAG(tasks), ErrInvalidWorkflow)
	})

	t.Run("valid diamond", func(t *testing.T) {
		tasks := []workflowTaskDef{
			{taskID: "A", args: wfArgs{id: "A"}},
			{taskID: "B", dependsOn: []string{"A"}, args: wfArgs{id: "B"}},
			{taskID: "C", dependsOn: []string{"A"}, args: wfArgs{id: "C"}},
			{taskID: "D", dependsOn: []string{"B", "C"}, args: wfArgs{id: "D"}},
		}
		require.NoError(t, validateWorkflowDAG(tasks))
	})
}
