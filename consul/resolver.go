package consul

import (
	"errors"
	"fmt"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/naming"
)

// Consul的地址解析, 实现grpc.naming.Resolver
type ConsulResolver struct {
	ServiceName string //服务名称
}

// 创建新的地址解析
func NewResolver(serviceName string) *ConsulResolver {
	return &ConsulResolver{ServiceName: serviceName}
}

// 从consul解析地址，target : consul的地址
func (cr *ConsulResolver) Resolve(target string) (naming.Watcher, error) {
	if cr.ServiceName == "" {
		return nil, errors.New("no service name provided")
	}

	// 创建一个新的consul客户端
	conf := &consul.Config{
		Scheme:  "http",
		Address: target,
	}
	client, err := consul.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("creat consul error: %v", err)
	}

	// return ConsulWatcher
	return &ConsulWatcher{
		consulResolver: cr,
		client: client,
	}, nil
}
