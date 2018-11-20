package balance

import (
	"errors"
	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc/naming"
	"log"
	"strings"
)

// etcd的地址解析, 实现grpc.naming.Resolver
type EtcdResolver struct {
	serviceName string //服务名称
}

// 创建新的地址解析
func NewResolver(serviceName string) *EtcdResolver {
	return &EtcdResolver{serviceName: serviceName}
}

// 从etcd解析地址，target : etcd的地址
func (et *EtcdResolver) Resolve(target string) (naming.Watcher, error) {
	if et.serviceName == "" {
		return nil, errors.New("no service name provided")
	}

	// 创建一个新的etcd客户端
	client, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(target, ","),
	})
	if err != nil {
		log.Printf("creat etcd3 client failed: %s", err.Error())
		return nil, err
	}

	// 返回 EtcdWatcher
	return &EtcdWatcher{
		resolver: et,
		client:   client,
	}, nil
}
