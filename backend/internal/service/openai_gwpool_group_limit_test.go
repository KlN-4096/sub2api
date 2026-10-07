//go:build unit

package service

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type gatewayPoolLimitGroupRepo struct {
	GroupRepository
	groups map[int64]*Group
	reads  int
}

func (r *gatewayPoolLimitGroupRepo) GetByIDLite(_ context.Context, id int64) (*Group, error) {
	r.reads++
	group := r.groups[id]
	if group == nil {
		return nil, ErrGroupNotFound
	}
	return group, nil
}

func TestGatewayPoolGroupLimitUsesActualGroupWithoutRepeatedReads(t *testing.T) {
	repo := &gatewayPoolLimitGroupRepo{groups: map[int64]*Group{
		8: {ID: 8, OpenAIGatewayPoolActiveAccounts: 3},
	}}
	svc := &OpenAIGatewayService{schedulerSnapshot: &SchedulerSnapshotService{groupRepo: repo}}
	authGroup, childGroup := int64(7), int64(8)
	ctx := context.WithValue(gatewayPoolTestGroupContext(authGroup, 2), gatewayPoolGroupLimitsKey{}, &gatewayPoolGroupLimits{})
	require.Equal(t, 2, svc.gatewayPoolActiveAccountLimit(ctx, &authGroup))
	require.Zero(t, repo.reads, "matching authenticated group already has this field")
	require.Equal(t, 3, svc.gatewayPoolActiveAccountLimit(ctx, &childGroup))
	require.Equal(t, 3, svc.gatewayPoolActiveAccountLimit(ctx, &childGroup))
	require.Equal(t, 1, repo.reads, "late claim must reuse the actual scheduling group's value")
	unknown := int64(9)
	require.Equal(t, 1, svc.gatewayPoolActiveAccountLimit(ctx, &unknown))
	require.Equal(t, 1, svc.gatewayPoolActiveAccountLimit(ctx, nil))
}

func TestGatewayPoolGroupLimitCreateUpdateDuplicateAndAuthCache(t *testing.T) {
	for _, value := range []int{0, 1, 2, 64, -1, 65} {
		t.Run(strconv.Itoa(value), func(t *testing.T) {
			repo := &groupRepoStubForAdmin{}
			svc := &adminServiceImpl{groupRepo: repo}
			group, err := svc.CreateGroup(context.Background(), &CreateGroupInput{
				Name: "offline", Platform: PlatformOpenAI, RateMultiplier: 1, OpenAIGatewayPoolActiveAccounts: &value,
			})
			if value < 1 || value > 64 {
				require.Error(t, err)
				require.Nil(t, repo.created)
			} else {
				require.NoError(t, err)
				require.Equal(t, value, group.OpenAIGatewayPoolActiveAccounts)
			}
		})
	}
	repo := &groupRepoStubForAdmin{}
	invalidator := &authCacheInvalidatorStub{}
	svc := &adminServiceImpl{groupRepo: repo, authCacheInvalidator: invalidator}
	group, err := svc.CreateGroup(context.Background(), &CreateGroupInput{Name: "default", Platform: PlatformOpenAI, RateMultiplier: 1})
	require.NoError(t, err)
	require.Equal(t, 1, group.OpenAIGatewayPoolActiveAccounts)
	group.ID, group.Hydrated = 7, true
	repo.getByID = group
	two := 2
	group, err = svc.UpdateGroup(context.Background(), group.ID, &UpdateGroupInput{OpenAIGatewayPoolActiveAccounts: &two})
	require.NoError(t, err)
	require.Equal(t, 2, group.OpenAIGatewayPoolActiveAccounts)
	require.Equal(t, []int64{group.ID}, invalidator.groupIDs)
	group, err = svc.UpdateGroup(context.Background(), group.ID, &UpdateGroupInput{Name: "keep"})
	require.NoError(t, err)
	require.Equal(t, 2, group.OpenAIGatewayPoolActiveAccounts, "an omitted edit field must not reset the limit")
	require.Equal(t, 2, cloneGroupForDuplicate(group, "offline-operation").OpenAIGatewayPoolActiveAccounts)
	apiKey := &APIKey{ID: 1, UserID: 1, GroupID: &group.ID, Key: "offline-key", Status: StatusActive,
		User: &User{ID: 1, Status: StatusActive}, Group: group}
	keys := &APIKeyService{}
	raw, err := json.Marshal(&APIKeyAuthCacheEntry{Snapshot: keys.snapshotFromAPIKey(context.Background(), apiKey)})
	require.NoError(t, err)
	var cached APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal(raw, &cached))
	restored, used, err := keys.applyAuthCacheEntry(apiKey.Key, &cached)
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, 2, restored.Group.OpenAIGatewayPoolActiveAccounts)
}
