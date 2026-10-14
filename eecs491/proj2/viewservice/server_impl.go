package viewservice

// additions to ViewServer state.
// in common.go
//type View struct {
//	Viewnum uint
//	Primary string
//	Backup  string

type VSRequests func(*ViewServerImpl) any
// TODO do i need "any"?
type ViewServerImpl struct {
	currView            View            // viewnum, primary, backup
	trackPings          map[string]int  // tracks all servers and its' pings
	primaryAcknowledged bool            // did the primary acknowledge the view?
	trackRestarted      map[string]bool // track the servers that have restarted

	VSRequests chan VSRequests // channel for viewservice operation requests
	VSResponses chan any // channel for ViewService responses --> who does this get sent to?
}

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	vs.impl.trackPings = make(map[string]int)
	vs.impl.currView = View{}
	vs.impl.primaryAcknowledged = false
	vs.impl.trackRestarted = make(map[string]bool)
	vs.impl.VSRequests = make(chan VSRequests)
	vs.impl.VSResponses = make(chan any)

	//go func() {
	//	for fn := range kvs.impl.VSRequests { // for the length of the channel having an action
	//		// referencing the struct's memory (the real one)
	//		// putting the result of the function in the response channel
	//		kvs.impl.VSResponses <- fn(&kvs.impl.VSRequests) ????
	//	}
	//}()
}

//type PingArgs struct {
//	Me      string // "host:port"
//	Viewnum uint   // caller's notion of current view #
//}

// Ping() RPC handler implementation
func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) {
	// if I received a ping, then the server is still working, so interval is at 0
	vs.impl.trackPings[args.Me] = 0
	if args.Viewnum == 0 && ((args.Me == vs.impl.currView.Backup) ||
		(args.Me == vs.impl.currView.Primary)) { // if the server is a primary or backup and the viewNum is 0
		// the server is either NEW or RESTARTED
		_, exists := vs.impl.trackRestarted[args.Me]
		if !exists { // the server is not in the map, so it's NEW
			vs.impl.trackRestarted[args.Me] = false
		} else {
			vs.impl.trackRestarted[args.Me] = true // server has been seen --> RESTARTED
		}
		// if it's the primary, then we should change the view and take the primary out
	}
	// if ping == 0 --> server restarted or is new
	//if args.Viewnum == 0 {
	//	// if the server is the primary, we need to ackonlwedge that it restarted and it's no longer safe as the primary
	//	// if the server is the backup... i guess it's fine, but it just needs to be synced again
	//}

	/// the primary server acknowledges the same view number and it's the primary
	if args.Me == vs.impl.currView.Primary && args.Viewnum == vs.impl.currView.Viewnum {
		vs.impl.primaryAcknowledged = true
	}
	reply.View = vs.impl.currView
}

// if view number form Ping is == 0, then it was restarted or new
// we should reply that the viewnum = currView

// Get() RPC handler implementation
func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) {
	reply.View = vs.impl.currView
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	// increment trackPings ++
	// if any server has trackPings > 5 then it's dead
	// when a tick is called, increase time it hasn't recieved a ping
	for key := range vs.impl.trackPings {
		vs.impl.trackPings[key]++
	}

	if vs.impl.trackPings[vs.impl.currView.Primary] >= DeadPings {

		// promote backup as new primary
		// promote any available idle server as backup
		newBackup := ""
		newPrimary := vs.impl.currView.Backup

		for key := range vs.impl.trackPings {
			if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
				vs.impl.trackPings[key] < DeadPings { // promote as a backup
				newBackup = key
				break
			}
		}
		// if no new backup was found then it's ""
		if vs.impl.primaryAcknowledged { // primary acknowledges the current view, we can upgrade
			vs.impl.currView = View{
				Viewnum: vs.impl.currView.Viewnum + 1,
				Primary: newPrimary,
				Backup:  newBackup,
			}
		}
	}

	if vs.impl.trackPings[vs.impl.currView.Backup] >= DeadPings {
		// primary is good, we jsut need a new backup
		newBackup := ""
		for key := range vs.impl.trackPings {
			if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
				vs.impl.trackPings[key] < DeadPings { // promote as a backup
				newBackup = key
				break
			}
		}
		// if no new backup was found then it's ""
		vs.impl.currView = View{
			Viewnum: vs.impl.currView.Viewnum + 1,
			Primary: vs.impl.currView.Primary, // primary stayed the same
			Backup:  newBackup,
		}
		vs.impl.primaryAcknowledged = false // reset acknowledgement since there's a new view
	}

	// SERVERS RESTARTED - TODO

	// DO I NEED TO TRACK ACKNOLWEDGEMETN omg yes wtf
	//	However, the view service must not promote the Backup in view i
	//	to be the Primary in view i+1 until the primary from view i acknowledges
	//	that it is operating in view i. The Primary does so by sending a Ping where
	//	the view number is set to i. The primary should only do so after it has
	//	successfully transferred its state to the Backup in view i.

}

// VIEW NUMBERS
/*
view number changes when
1) primary dies and backup is promoted
2) backkup dies and idle is promoted
3) if the ping is 0, the server restarted, but the view shoudl still change
*/
