package balance

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/naming"
	"log"
)

// EtcdWatcher监听服务的变更，添加或者删除，实现grpc.naming.Watcher
type EtcdWatcher struct {
	resolver      *EtcdResolver
	client        *clientv3.Client
	isInitialized bool
}

func (*EtcdWatcher) Close() {}

func (ew *EtcdWatcher) Next() ([]*naming.Update, error) {
	prefix := fmt.Sprintf("/%s/%s/", PREFIX, ew.resolver.serviceName)
	if !ew.isInitialized {
		// 从etcd获取地址
		ew.isInitialized = true
		addrs, err := queryAddress(ew, prefix)
		if err != nil {
			return nil, err
		}
		// 不为空，添加并监听
		if l := len(addrs); l != 0 {
			updates := make([]*naming.Update, l)
			for i := range addrs {
				updates[i] = &naming.Update{Op: naming.Add, Addr: addrs[i]}
			}
			return updates, nil
		}
	}

	// 监听
	watcherChannel := ew.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for watchResponse := range watcherChannel {
		for _, ev := range watchResponse.Events {
			switch ev.Type {
			case mvccpb.PUT:
				return []*naming.Update{{Op: naming.Add, Addr: string(ev.Kv.Value)}}, nil
			case mvccpb.DELETE:
				return []*naming.Update{{Op: naming.Delete, Addr: string(ev.Kv.Value)}}, nil
			}
		}
	}

	// 正常情况不应该返回这个
	return []*naming.Update{}, nil
}

//从etcd查询service的address
func queryAddress(ew *EtcdWatcher, prefix string) ([]string, error) {
	var addrs []string
	resp, err := ew.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		log.Println("get etcd key value failed, err:", err.Error())
		return nil, err
	}

	if resp == nil || resp.Kvs == nil {
		return addrs, nil
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			addrs = append(addrs, string(v))
		}
	}

	return addrs, nil
}
