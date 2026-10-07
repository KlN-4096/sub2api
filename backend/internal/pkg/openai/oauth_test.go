package openai

import (
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestSessionStore_Stop_Idempotent(t *testing.T) {
	store := NewSessionStore()

	store.Stop()
	store.Stop()

	select {
	case <-store.stopCh:
		// ok
	case <-time.After(time.Second):
		t.Fatal("stopCh 未关闭")
	}
}

func TestSessionStore_Stop_Concurrent(t *testing.T) {
	store := NewSessionStore()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Stop()
		}()
	}

	wg.Wait()

	select {
	case <-store.stopCh:
		// ok
	case <-time.After(time.Second):
		t.Fatal("stopCh 未关闭")
	}
}

func TestBuildAuthorizationURLForPlatform_OpenAI(t *testing.T) {
	authURL := BuildAuthorizationURLForPlatform("state-1", "challenge-1", DefaultRedirectURI, OAuthPlatformOpenAI, CodexDefaultOriginator)
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("Parse URL failed: %v", err)
	}
	q := parsed.Query()
	if got := q.Get("client_id"); got != ClientID {
		t.Fatalf("client_id mismatch: got=%q want=%q", got, ClientID)
	}
	if got := q.Get("codex_cli_simplified_flow"); got != "true" {
		t.Fatalf("codex flow mismatch: got=%q want=true", got)
	}
	if got := q.Get("id_token_add_organizations"); got != "true" {
		t.Fatalf("id_token_add_organizations mismatch: got=%q want=true", got)
	}
	if got := q.Get("originator"); got != CodexDefaultOriginator {
		t.Fatalf("originator mismatch: got=%q want=%q", got, CodexDefaultOriginator)
	}
	// 真客户端（codex-rs 16ff14c login/src/server.rs:588-591）还带 api.connectors.read /
	// api.connectors.invoke；网关有意不扩权，期望值写死，改常量必须让这里失败。
	if got := q.Get("scope"); got != "openid profile email offline_access" {
		t.Fatalf("scope must not expand: got=%q", got)
	}
}

func TestBuildAuthorizationURLIdentityEncoding(t *testing.T) {
	for _, originator := range []string{"", "codex_cli_rs", "client&scope=extra"} {
		t.Run(originator, func(t *testing.T) {
			const redirect = "http://localhost:1455/auth/callback?state=a+b"
			raw := BuildAuthorizationURLForPlatform("state&value", "challenge+value", redirect, OAuthPlatformOpenAI, originator)
			parsed, err := url.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			want := originator
			if want == "" {
				want = CodexDefaultOriginator
			}
			q := parsed.Query()
			if q.Get("originator") != want || q.Get("scope") != DefaultScopes ||
				q.Get("state") != "state&value" || q.Get("code_challenge") != "challenge+value" ||
				q.Get("redirect_uri") != redirect {
				t.Fatalf("authorization parameters did not round-trip: %v", q)
			}
		})
	}
	parsed, err := url.Parse(BuildAuthorizationURL("state", "challenge", ""))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("originator") != CodexDefaultOriginator || parsed.Query().Get("redirect_uri") != DefaultRedirectURI {
		t.Fatal("default authorization wrapper lost its identity or redirect")
	}
}
