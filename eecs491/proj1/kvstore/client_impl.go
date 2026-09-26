package kvstore

// Perform an operation
//
// The operation must keep trying until the key/value
// server replies, or the client has been shut down.
func (ck *KVClerk) doOperation(op Op, key string,
	value string, reply *OpReply) {

		operation := OpArgs{
			Op: op, // Get, Put, Append
			Key: key, // key being fetched or modified
			Value: value, // value to be put or appended
			SeqNo: ck.SeqNum(), // sequence number of the operation from this client
			Client: ck.me, // the client making the request: Clerk 
		}

		for {
			response := call(ck.server, "KVServer.Operation", operation, &reply) // client sends request through RPC
			if response {
				return
			}
		}
}
// type Err string

// const (
// 	OK            = "OK"       // Success
// 	ErrNoKey      = "ErrNoKey" // No key (Get only)
// 	ErrTerminated = "ErrTerminated"
// )

// ck.server = server
// get sequence number with ck.SeqNum()


// func call(srv string, rpcname string,
// 	args interface{}, reply interface{}) bool {
// i'm confused what rpcname is and what it woudl be for the parameter


// “A correct client implementation, once given an Operation 
// to perform, will repeatedly attempt to perform that operation 
// until either the client is shut down, or the Operation RPC returns successfully.”


// attempt to perform that operation: but it's not doing it, we have to send a request to the server to do it?
// oh so i should do a Call() so that it can do an RPC for the server to do the work 