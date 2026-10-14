package pbservice

// additions to PBServer state.
type PBServerImpl struct {
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
}

// server Operation() RPC handler.
func (pb *PBServer) OperationImpl(args OpArgs, reply *OpReply) {
}

// server Push() RPC handler
func (pb *PBServer) PushImpl(args PushArgs, reply *PushReply) {
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
}
