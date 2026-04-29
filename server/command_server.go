package server

import (
	"godis/api"
	"godis/cluster"
)

type CommandServer struct {
	ClusterState *cluster.ClusterState
}


func (cs *CommandServer) SetSlot (args *api.SetSlotArgs){
	cs.ClusterState.EventQueue<- &cluster.SetSlot{}
}

func (cs * CommandServer)MigrateKeys(args *api.MigrateKeysArgs ,reply *api.MigrateKeysReply){
	cs.ClusterState.ClusterMigrateKeys(args,reply)
}

func (cs *CommandServer)Restore(args *api.RestoreArgs ,reply *api.RestoreReply){
	cs.ClusterState.ClusterRestore(args,reply)
}