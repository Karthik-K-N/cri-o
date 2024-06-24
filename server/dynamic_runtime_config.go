package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/containers/nri-plugins/pkg/log"
	"github.com/containers/nri-plugins/pkg/sysfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	types "k8s.io/cri-api/pkg/apis/runtime/v1"
)
 
type dynamicRuntimeConfigConn struct {
	wg  sync.WaitGroup
	err error
}

// GetDynamicRuntimeConfig gets runtime configurations from the CRI runtime
func (s *Server) GetDynamicRuntimeConfig(_ *types.DynamicRuntimeConfigRequest, runtimeConfigServer types.RuntimeService_GetDynamicRuntimeConfigServer) error {
	s.dynamicRuntimeConfigBroadcaster.Do(func() {
		go s.broadcastRuntimeConfig()
	})
	conn := &dynamicRuntimeConfigConn{
		wg: sync.WaitGroup{},
	}

	s.dynamicRuntimeConfigClients.Store(runtimeConfigServer, conn)
	conn.wg.Add(1)

	// wait here until we don't want to send events to this client anymore
	conn.wg.Wait()
	s.dynamicRuntimeConfigClients.Delete(runtimeConfigServer)
	return conn.err
}

func (s *Server) broadcastRuntimeConfig() {
	// notify all connections that dynamicRuntimeConfigClients has been closed
	defer s.dynamicRuntimeConfigClients.Range(func(_, value any) bool { // nolint: unparam
		conn, ok := value.(*dynamicRuntimeConfigConn)
		if !ok {
			return true
		}
		conn.wg.Done()
		return true
	})

	for runtimeConfig := range s.DynamicRuntimeConfigChan {
		s.dynamicRuntimeConfigClients.Range(func(key, value any) bool {
			stream, ok := key.(types.RuntimeService_GetDynamicRuntimeConfigServer)
			if !ok {
				return true
			}
			conn, ok := value.(*dynamicRuntimeConfigConn)
			if !ok {
				return true
			}

			if err := stream.Send(&runtimeConfig); err != nil {
				code, _ := status.FromError(err)
				// when the client closes the connection this error is expected
				// so only log non transport closing errors
				if code.Code() != codes.Unavailable && code.Message() != "transport is closing" {
					conn.err = err
				}
				// notify our waiting client connection that we are done
				conn.wg.Done()
			}
			return true
		})
	}
}

func (s *Server) fetchMachineInfo() {
	ticker := time.NewTicker(time.Second * 30)
	for ; ; <-ticker.C {
		fmt.Println("Fetching the  machine info...")
		sys, e := sysfs.DiscoverSystem()
		if e != nil {
			panic(e)
		}
		zone := &types.ResourceTopologyZone{}
		for _, nodeID := range sys.NodeIDs() {
			zone = &types.ResourceTopologyZone{
				Name: fmt.Sprintf("Node-%d", nodeID),
				Type: types.ResourceTopologyZoneType_NUMANODE,
				Resources: []*types.ResourceTopologyResourceInfo{
					{
						Name:     "cpu",
						Capacity: &types.Quantity{String_: fmt.Sprintf("%d", sys.CPUCount())},
					},
				},
			}

			node := sys.Node(nodeID)
			memoryInfo, err := node.MemoryInfo()
			if err != nil {
				log.Error("Error fetching node memory info", err)
				panic(err)
			} else {
				q := resource.NewQuantity(int64(memoryInfo.MemTotal), resource.DecimalSI)
				memoryResource := &types.ResourceTopologyResourceInfo{
					Name: "memory",
					Capacity: &types.Quantity{
						String_: q.String(),
					},
				}
				zone.Resources = append(zone.Resources, memoryResource)
			}
		}

		dynamicResourceConfig := types.DynamicRuntimeConfigResponse{
			ResourceTopology: &types.ResourceTopology{
				Zones: []*types.ResourceTopologyZone{
					zone,
				},
			},
		}
		s.DynamicRuntimeConfigChan <- dynamicResourceConfig
	}

}
