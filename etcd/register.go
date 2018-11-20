package balance

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const PREFIX = "etcd3_naming"

/*
Register : 注册服务到etcd
	name : 服务名
	host : 服务地址
	port : 服务端口
	target: etcd的Dial地址 如"127.0.0.1:2379"
	dialTimeout : 超时时间
	ttl : 注册信息的生存周期
*/

func Register(name string, host string, port int, target string, dialTimeout time.Duration, ttl int) error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(target, ","),
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Printf("create etcd client error: %v", err)
		return err
	}

	serviceValue := fmt.Sprintf("%s:%d", host, port)
	serviceKey := fmt.Sprintf("/%s/%s/%s", PREFIX, name, serviceValue)

	leaseGrantResponse, err := client.Grant(context.TODO(), int64(ttl))
	if err != nil {
		log.Printf("get etcd lease grant response error: %v", err)
		return err
	}

	_, err = client.Put(context.TODO(), serviceKey, serviceValue, clientv3.WithLease(leaseGrantResponse.ID))
	if err != nil {
		log.Printf("put etcd key value failed, error: %s", err.Error())
		return err
	}

	leaseKeepAliveResponse, err := client.KeepAlive(context.TODO(), leaseGrantResponse.ID)
	if err != nil {
		log.Printf("get etcd lease grant response error: %v", err)
		return err
	}

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT)
		for {
			select {
			//从channel取数据防止queue满，另一个监听keepAlive
			case _, ok := <-leaseKeepAliveResponse:
				if !ok {
					log.Println("get leaseKeepAliveResponse from channel failed")
				}
			case signal := <-ch:
				log.Println("receive signal: ", signal)
				Revoke(client, leaseGrantResponse.ID)
				s, _ := strconv.Atoi(fmt.Sprintf("%d", signal))
				os.Exit(s)
			case <-client.Ctx().Done():
				log.Println("server close")
				break
			}
		}
	}()

	return nil
}

func Revoke(client *clientv3.Client, leaseId clientv3.LeaseID) error {
	_, err := client.Revoke(context.TODO(), leaseId)
	if err != nil {
		log.Printf("revoke etcd failed, error: %s", err.Error())
		return err
	}
	return nil
}
