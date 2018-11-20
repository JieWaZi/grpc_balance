package consul

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	consul "github.com/hashicorp/consul/api"
)
/*
Register : 注册服务到Consul
	name : 服务名
	host : 服务地址
	port : 服务端口
	target: consul的Dial地址 如"127.0.0.1:8500"
	interval : 向consul注册的间隔
	ttl : 注册信息的生存周期
*/

func Register(name string, host string, port int, target string, interval time.Duration, ttl int) error {
	conf := &consul.Config{Scheme: "http", Address: target}
	client, err := consul.NewClient(conf)
	if err != nil {
		return fmt.Errorf("create consul client error: %v", err)
	}

	serviceID := fmt.Sprintf("%s-%s-%d", name, host, port)

	//注销服务当接收到相关信号时，例如kill，quit...
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT)
		x := <-ch
		log.Println("receive signal: ", x)

		err := client.Agent().ServiceDeregister(serviceID)
		if err != nil {
			log.Println("deregister service error: ", err.Error())
		} else {
			log.Println(fmt.Sprintf("deregistered %s from consul server.", name))
		}

		err = client.Agent().CheckDeregister(serviceID)
		if err != nil {
			log.Println("deregister check error: ", err.Error())
		}

		s, _ := strconv.Atoi(fmt.Sprintf("%d", x))

		os.Exit(s)
	}()

	// 更新ttl，使用定时器每隔给定间隔就更新ttl，使consul能够监控该server的健康状况
	go func() {
		ticker := time.NewTicker(interval)
		for {
			<-ticker.C
			err = client.Agent().UpdateTTL(serviceID, "", "passing")
			if err != nil {
				log.Println("update ttl of service error: ", err.Error())
			}
		}
	}()

	// 服务信息配置
	registration := &consul.AgentServiceRegistration{
		ID:      serviceID,
		Name:    name,
		Address: host,
		Port:    port,
	}
	err = client.Agent().ServiceRegister(registration)
	if err != nil {
		return fmt.Errorf("initial register service '%s' host to consul error: %s", name, err.Error())
	}

	// consul健康检测
	check := consul.AgentServiceCheck{TTL: fmt.Sprintf("%ds", ttl), Status: "passing"}
	err = client.Agent().CheckRegister(&consul.AgentCheckRegistration{ID: serviceID, Name: name, ServiceID: serviceID, AgentServiceCheck: check})
	if err != nil {
		return fmt.Errorf("initial register service check to consul error: %s", err.Error())
	}

	return nil
}
