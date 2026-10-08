package repository

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var refreshConcurrencySlotScript = redis.NewScript(`
	redis.replicate_commands()
	local now = tonumber(redis.call('TIME')[1])
	local ttl = tonumber(ARGV[1])
	local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
	if not score or tonumber(score) <= now - ttl then
		return 0
	end
	redis.call('ZADD', KEYS[1], now, ARGV[2])
	redis.call('EXPIRE', KEYS[1], ttl)
	if #KEYS > 1 then
		redis.call('ZADD', KEYS[2], now + ttl, ARGV[3])
	end
	return 1
`)

func (c *concurrencyCache) RefreshConcurrencySlot(ctx context.Context, kind string, id int64, requestID string) (bool, error) {
	if c == nil || c.rdb == nil || id <= 0 || requestID == "" {
		return false, nil
	}
	var keys []string
	switch kind {
	case "account":
		keys = []string{accountSlotKey(id), accountActiveIndexKey}
	case "user":
		keys = []string{userSlotKey(id), userActiveIndexKey}
	case "api_key":
		keys = []string{apiKeySlotKey(id)}
	default:
		return false, nil
	}
	result, err := refreshConcurrencySlotScript.Run(ctx, c.rdb, keys,
		c.slotTTLSeconds, requestID, strconv.FormatInt(id, 10)).Int()
	return result == 1, err
}
