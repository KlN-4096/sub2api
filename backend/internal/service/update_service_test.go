//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type updateServiceCacheStub struct {
	data string
}

func (s *updateServiceCacheStub) GetUpdateInfo(context.Context) (string, error) {
	if s.data == "" {
		return "", errors.New("cache miss")
	}
	return s.data, nil
}

func (s *updateServiceCacheStub) SetUpdateInfo(_ context.Context, data string, _ time.Duration) error {
	s.data = data
	return nil
}

type updateServiceGitHubClientStub struct {
	release        *GitHubRelease
	recentReleases []*GitHubRelease
	recentErr      error
}

func (s *updateServiceGitHubClientStub) FetchLatestRelease(context.Context, string) (*GitHubRelease, error) {
	return s.release, nil
}

func (s *updateServiceGitHubClientStub) FetchRecentReleases(context.Context, string, int) ([]*GitHubRelease, error) {
	return s.recentReleases, s.recentErr
}

func (s *updateServiceGitHubClientStub) DownloadFile(context.Context, string, string, int64) error {
	panic("DownloadFile should not be called when no update is available")
}

func (s *updateServiceGitHubClientStub) FetchChecksumFile(context.Context, string) ([]byte, error) {
	panic("FetchChecksumFile should not be called when no update is available")
}

func TestUpdateServicePerformUpdateNoUpdateReturnsSentinel(t *testing.T) {
	svc := NewUpdateService(
		&updateServiceCacheStub{},
		&updateServiceGitHubClientStub{
			release: &GitHubRelease{
				TagName: "v0.1.132",
				Name:    "v0.1.132",
			},
		},
		"0.1.132",
		"release",
	)

	err := svc.PerformUpdate(context.Background())

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNoUpdateAvailable))
	require.ErrorIs(t, err, ErrNoUpdateAvailable)
}

func newRollbackTestService(current string, releases []*GitHubRelease) *UpdateService {
	return NewUpdateService(
		&updateServiceCacheStub{},
		&updateServiceGitHubClientStub{recentReleases: releases},
		current,
		"release",
	)
}

func TestUpdateServiceListRollbackVersionsFiltersAndCaps(t *testing.T) {
	releases := []*GitHubRelease{
		{TagName: "v0.1.148", PublishedAt: "2026-07-09T00:00:00Z"},                       // newer than current: excluded
		{TagName: "v0.1.147", PublishedAt: "2026-07-08T00:00:00Z"},                       // current: excluded
		{TagName: "v0.1.146-rc1", PublishedAt: "2026-07-07T12:00:00Z", Prerelease: true}, // prerelease: excluded
		{TagName: "v0.1.146", PublishedAt: "2026-07-07T00:00:00Z"},
		{TagName: "v0.1.145", PublishedAt: "2026-07-06T00:00:00Z", Draft: true}, // draft: excluded
		{TagName: "v0.1.144", PublishedAt: "2026-07-05T00:00:00Z"},
		{TagName: "v0.1.144", PublishedAt: "2026-07-05T00:00:00Z"}, // duplicate: excluded
		{TagName: "v0.1.143", PublishedAt: "2026-07-04T00:00:00Z"},
		{TagName: "v0.1.142", PublishedAt: "2026-07-03T00:00:00Z"}, // beyond cap of 3: excluded
	}
	svc := newRollbackTestService("0.1.147", releases)

	versions, err := svc.ListRollbackVersions(context.Background())

	require.NoError(t, err)
	require.Len(t, versions, 3)
	require.Equal(t, "0.1.146", versions[0].Version)
	require.Equal(t, "0.1.144", versions[1].Version)
	require.Equal(t, "0.1.143", versions[2].Version)
}

func TestUpdateServiceListRollbackVersionsSortsUnorderedInput(t *testing.T) {
	releases := []*GitHubRelease{
		{TagName: "v0.1.144"},
		{TagName: "v0.1.146"},
		{TagName: "v0.1.145"},
	}
	svc := newRollbackTestService("0.1.147", releases)

	versions, err := svc.ListRollbackVersions(context.Background())

	require.NoError(t, err)
	require.Len(t, versions, 3)
	require.Equal(t, "0.1.146", versions[0].Version)
	require.Equal(t, "0.1.145", versions[1].Version)
	require.Equal(t, "0.1.144", versions[2].Version)
}

func TestUpdateServiceListRollbackVersionsEmptyWhenNoneOlder(t *testing.T) {
	releases := []*GitHubRelease{
		{TagName: "v0.1.147"},
		{TagName: "v0.1.148"},
	}
	svc := newRollbackTestService("0.1.147", releases)

	versions, err := svc.ListRollbackVersions(context.Background())

	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestUpdateServiceListRollbackVersionsPropagatesFetchError(t *testing.T) {
	svc := NewUpdateService(
		&updateServiceCacheStub{},
		&updateServiceGitHubClientStub{recentErr: errors.New("github unavailable")},
		"0.1.147",
		"release",
	)

	_, err := svc.ListRollbackVersions(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "github unavailable")
}

func TestUpdateServiceRollbackToVersionRejectsDisallowedTargets(t *testing.T) {
	releases := []*GitHubRelease{
		{TagName: "v0.1.148"},
		{TagName: "v0.1.147"},
		{TagName: "v0.1.146"},
		{TagName: "v0.1.145"},
		{TagName: "v0.1.144"},
		{TagName: "v0.1.143"},
		{TagName: "v0.1.142"},
	}
	svc := newRollbackTestService("0.1.147", releases)

	for _, target := range []string{
		"",         // empty
		"0.1.147",  // current version
		"v0.1.147", // current version with prefix
		"0.1.148",  // newer than current
		"0.1.142",  // older than the 3 most recent
		"9.9.9",    // nonexistent
	} {
		err := svc.RollbackToVersion(context.Background(), target)
		require.ErrorIs(t, err, ErrRollbackVersionNotAllowed, "target %q should be rejected", target)
	}
}

func TestUpdateServiceRollbackToVersionAcceptsVPrefix(t *testing.T) {
	// No platform asset in the release: the target passes the allowlist check
	// and fails later at asset lookup, proving the version itself was accepted.
	releases := []*GitHubRelease{
		{TagName: "v0.1.147"},
		{TagName: "v0.1.146"},
	}
	svc := newRollbackTestService("0.1.147", releases)

	err := svc.RollbackToVersion(context.Background(), "v0.1.146")

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRollbackVersionNotAllowed)
	require.Contains(t, err.Error(), "no compatible release found")
}

// updateServiceRepoStub 按仓库分别返回 release，并记录被查询的仓库。
type updateServiceRepoStub struct {
	updateServiceGitHubClientStub
	latestByRepo map[string]*GitHubRelease
	recentByRepo map[string][]*GitHubRelease
	calls        []string
}

func (s *updateServiceRepoStub) FetchLatestRelease(_ context.Context, repo string) (*GitHubRelease, error) {
	s.calls = append(s.calls, "latest:"+repo)
	if r, ok := s.latestByRepo[repo]; ok {
		return r, nil
	}
	return nil, errors.New("unexpected repo " + repo)
}

func (s *updateServiceRepoStub) FetchRecentReleases(_ context.Context, repo string, _ int) ([]*GitHubRelease, error) {
	s.calls = append(s.calls, "recent:"+repo)
	return s.recentByRepo[repo], nil
}

func TestUpdateServiceForkPatchReleaseIsAnUpdate(t *testing.T) {
	gh := &updateServiceRepoStub{latestByRepo: map[string]*GitHubRelease{
		"KlN-4096/sub2api": {TagName: "v0.2.7-klno.5", HTMLURL: "https://github.com/KlN-4096/sub2api/releases/tag/v0.2.7-klno.5"},
		"Wei-Shaw/sub2api": {TagName: "v0.2.7", HTMLURL: "https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.7"},
	}}
	cache := &updateServiceCacheStub{}
	svc := NewUpdateService(cache, gh, "0.2.7-klno.4", "release")

	info, err := svc.CheckUpdate(context.Background(), true)

	require.NoError(t, err)
	require.True(t, info.HasUpdate, "klno.4 -> klno.5 must count as an update")
	require.Equal(t, "0.2.7-klno.5", info.LatestVersion)
	require.NotNil(t, info.Upstream)
	require.Equal(t, "0.2.7", info.Upstream.CurrentVersion)
	require.Equal(t, "0.2.7", info.Upstream.LatestVersion)
	require.False(t, info.Upstream.HasUpdate)
	require.ElementsMatch(t, []string{"latest:KlN-4096/sub2api", "latest:Wei-Shaw/sub2api"}, gh.calls)

	cached, err := svc.CheckUpdate(context.Background(), false)

	require.NoError(t, err)
	require.True(t, cached.Cached)
	require.True(t, cached.HasUpdate)
	require.NotNil(t, cached.Upstream)
	require.Equal(t, "0.2.7", cached.Upstream.LatestVersion)
	require.Equal(t, "https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.7", cached.Upstream.HTMLURL)
}

func TestUpdateServiceNeverUpdatesFromUpstream(t *testing.T) {
	// 上游已发 0.2.8、二开还没跟上：只提示上游有新版，一键升级不能拿上游的包。
	// DownloadFile 被调用会 panic（updateServiceGitHubClientStub）。
	gh := &updateServiceRepoStub{latestByRepo: map[string]*GitHubRelease{
		"KlN-4096/sub2api": {TagName: "v0.2.7-klno.4"},
		"Wei-Shaw/sub2api": {TagName: "v0.2.8", Assets: []GitHubAsset{
			{Name: "sub2api_0.2.8_linux_amd64.tar.gz", BrowserDownloadURL: "https://github.com/Wei-Shaw/sub2api/releases/download/v0.2.8/sub2api_0.2.8_linux_amd64.tar.gz"},
			{Name: "checksums.txt", BrowserDownloadURL: "https://github.com/Wei-Shaw/sub2api/releases/download/v0.2.8/checksums.txt"},
		}},
	}}
	svc := NewUpdateService(&updateServiceCacheStub{}, gh, "0.2.7-klno.4", "release")

	info, err := svc.CheckUpdate(context.Background(), true)
	require.NoError(t, err)
	require.False(t, info.HasUpdate)
	require.True(t, info.Upstream.HasUpdate)
	require.Equal(t, "0.2.8", info.Upstream.LatestVersion)

	require.ErrorIs(t, svc.PerformUpdate(context.Background()), ErrNoUpdateAvailable)
}

func TestUpdateServiceUpstreamFailureKeepsForkResult(t *testing.T) {
	gh := &updateServiceRepoStub{latestByRepo: map[string]*GitHubRelease{
		"KlN-4096/sub2api": {TagName: "v0.2.7-klno.5"},
	}}
	svc := NewUpdateService(&updateServiceCacheStub{}, gh, "0.2.7-klno.4", "release")

	info, err := svc.CheckUpdate(context.Background(), true)

	require.NoError(t, err)
	require.True(t, info.HasUpdate)
	require.NotNil(t, info.Upstream)
	require.False(t, info.Upstream.HasUpdate)
	require.NotEmpty(t, info.Upstream.Warning)
}

func TestUpdateServiceRollbackUsesForkReleasesInKlnoOrder(t *testing.T) {
	gh := &updateServiceRepoStub{recentByRepo: map[string][]*GitHubRelease{
		"KlN-4096/sub2api": {
			{TagName: "v0.2.7-klno.5"}, // newer than current: excluded
			{TagName: "v0.2.7-klno.4"}, // current: excluded
			{TagName: "v0.2.5-klno.13"},
			{TagName: "v0.2.7-klno.1"},
			{TagName: "v0.2.5-klno.9"},
			{TagName: "v0.2.7-klno.3"},
			{TagName: "v0.2.7-klno.2"},
		},
		"Wei-Shaw/sub2api": {{TagName: "v0.2.7"}, {TagName: "v0.2.6"}},
	}}
	svc := NewUpdateService(&updateServiceCacheStub{}, gh, "0.2.7-klno.4", "release")

	versions, err := svc.ListRollbackVersions(context.Background())

	require.NoError(t, err)
	require.Len(t, versions, 3)
	require.Equal(t, "0.2.7-klno.3", versions[0].Version)
	require.Equal(t, "0.2.7-klno.2", versions[1].Version)
	require.Equal(t, "0.2.7-klno.1", versions[2].Version)
	require.Equal(t, []string{"recent:KlN-4096/sub2api"}, gh.calls)
}

func TestCompareVersionsKlnoSuffix(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0.2.7-klno.4", "0.2.7-klno.5", -1},
		{"0.2.7-klno.13", "0.2.7-klno.9", 1}, // 数值比较，不是字典序
		{"0.2.7", "0.2.7-klno.1", -1},        // 源码构建（VERSION 无后缀）低于任何 klno 发布
		{"0.2.8", "0.2.7-klno.13", 1},
		{"v0.2.7-klno.4", "0.2.7-klno.4", 0},
		{"0.1.146-rc1", "0.1.146", 0}, // 非 klno 后缀维持旧语义：忽略
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, compareVersions(tc.a, tc.b), "%s vs %s", tc.a, tc.b)
	}
}
