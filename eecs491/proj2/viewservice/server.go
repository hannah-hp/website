package viewservice

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	l        net.Listener
	rpccount int32 // for testing
	me       string
	term     chan interface{}
	impl     ViewServerImpl
}

// Shut down the server
// Pass the termination channel used in initialization
func (vs *ViewServer) Kill() {
	fmt.Printf("Killing viewserver %v\n", vs.me)
	close(vs.term)
	vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
	select {
	case <-vs.term:
		return true
	default:
		return false
	}
}

func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.term = make(chan interface{})
	vs.me = me
	vs.initImpl()

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// create a thread to accept RPC connections from clients.
	go func() {
		defer vs.l.Close()
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			// We may have been killed while waiting for a new request
			if vs.isdead() {
				if err == nil {
					conn.Close()
				}
				return
			}
			// If this accept resulted in an error, log it and try again
			if err != nil {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				continue
			}
			// We are not dead, and have a valid connection
			// Serve it asynchronously
			atomic.AddInt32(&vs.rpccount, 1)
			go rpcs.ServeConn(conn)
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

// Ping Wrapper
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	if vs.isdead() {
		errString := "Server " + vs.me + " is dead"
		return errors.New(errString)
	} else {
		repChan := make(chan PingReply, 1) // Buffered in case we are throwing this away
		go func() {
			var innerReply PingReply
			vs.PingImpl(args, &innerReply)
			repChan <- innerReply // Guaranteed not to stop, even if server is dead
		}()

		select {
		case *reply = <-repChan:
		case <-vs.term:
			errString := "Server " + vs.me + " is dead"
			return errors.New(errString)
		}
	}
	return nil
}

// Get Wrapper
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	if vs.isdead() {
		errString := "Server " + vs.me + " is dead"
		return errors.New(errString)
	} else {
		repChan := make(chan GetReply, 1)
		go func() {
			var innerReply GetReply
			vs.GetImpl(args, &innerReply)
			repChan <- innerReply
		}()

		select {
		case *reply = <-repChan:
		case <-vs.term:
			errString := "Server " + vs.me + " is dead"
			return errors.New(errString)
		}
	}
	return nil
}
