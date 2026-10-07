package service

import "fmt"

var errGatewayPoolPreflightChanged = fmt.Errorf("%w: route changed before business send", errOpenAIGatewayPoolWarmUnverified)
