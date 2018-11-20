package consul

import (
	"fmt"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/naming"
)

// ConsulWatcher监听服务的变更，添加或者删除，实现grpc.naming.Watcher
type ConsulWatcher struct {

	consulResolver *ConsulResolver

	client *consul.Client

	lastIndex uint64

	// addrs 是consul缓存的addrs
	// check之前: 所有指为 1
	// check之后: 1 是 deleted  2 是 nothing  3 是 new added
	addrs []string
}

func (cw *ConsulWatcher) Close() {
}

func (cw *ConsulWatcher) Next() ([]*naming.Update, error) {
	// cw.addrs == Nil  表示第一次调用
	// 如果没查到地址，将一直监听查询

	if len(cw.addrs) == 0 {
		// 查询Consul中的地址,并将得到的地址返回给负载均衡
		addrs, li, _ := cw.queryConsul(nil)

		if len(addrs) != 0 {
			cw.addrs = addrs
			cw.lastIndex = li
			return Updates([]string{}, addrs), nil
		}
	}

	//查询地址，没有查到地址将一直查询直到查到为止
	for {
		addrs, li, err := cw.queryConsul(&consul.QueryOptions{WaitIndex: cw.lastIndex})
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		updates := Updates(cw.addrs, addrs)

		cw.addrs = addrs
		cw.lastIndex = li

		if len(updates) != 0 {
			return updates, nil
		}
	}

	// 正常情况不应该返回这个
	return []*naming.Update{}, nil
}

// queryConsul 查询consul
func (cw *ConsulWatcher) queryConsul(q *consul.QueryOptions) ([]string, uint64, error) {
	serviceEntrys, meta, err := cw.client.Health().Service(cw.consulResolver.ServiceName, "", true, q)
	if err != nil {
		return nil, 0, err
	}

	addrs := make([]string, 0)
	for _, serviceEntry := range serviceEntrys {
		addrs = append(addrs, fmt.Sprintf("%s:%d", serviceEntry.Service.Address, serviceEntry.Service.Port))
	}

	return addrs, meta.LastIndex, nil
}

func Updates(a, b []string) []*naming.Update {
	updates := []*naming.Update{}

	deleted := getDiff(a, b)
	for _, addr := range deleted {
		update := &naming.Update{Op: naming.Delete, Addr: addr}
		updates = append(updates, update)
	}

	added := getDiff(b, a)
	for _, addr := range added {
		update := &naming.Update{Op: naming.Add, Addr: addr}
		updates = append(updates, update)
	}
	return updates
}

func getDiff(a, b []string) []string {
	diff := make([]string, 0)
	for _, va := range a {
		found := false
		for _, vb := range b {
			if va == vb {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, va)
		}
	}
	return diff
}
