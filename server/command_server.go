package server

import (
	"godis/api"
	"godis/cluster"
)

type CommandServer struct {
	cs *cluster.ClusterState
}


func (cs *CommandServer) SetSlot (args *api.SetSlotArgs){
	cs.cs.ClusterSetSlot(args)
}

func (cs * CommandServer)MigrateKeys(args *api.MigrateKeysArgs ,reply *api.MigrateKeysReply){
	cs.cs.ClusterMigrateKeys(args,reply)
}

func (cs *CommandServer)Restore(args *api.RestoreArgs ,reply *api.RestoreReply){
	cs.cs.ClusterRestore(args,reply)
}