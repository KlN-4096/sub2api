//go:build unit

package service

// SetCodexModelsURLForTest 把 /models 清单端点指到本地假上游（service 包之外的离线用例，如管理端
// handler 的探针接线用例），返回还原函数。只编进 unit 构建，不进正式二进制。
func SetCodexModelsURLForTest(url string) (restore func()) {
	original := chatgptCodexModelsURL
	chatgptCodexModelsURL = url
	return func() { chatgptCodexModelsURL = original }
}
