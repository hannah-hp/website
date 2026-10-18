package viewservice

type VSRequests func(*ViewServerImpl) any

type ViewServerImpl struct {
	currView            View            // viewnum, primary, backup
	seenNonZero         map[string]bool // tracking if the server has seen nonzero viewNums
	trackPings          map[string]uint // tracks all servers and its' pings
	primaryAcknowledged bool            // did the primary acknowledge the view?
	trackRestarted      map[string]bool // track the servers that have restarted
	VSRequests          chan VSRequests // channel for viewservice operation requests
	VSResponses         chan any        // channel for ViewService responses
	lastViewNum         map[string]uint // tracks all servers and its' last viewnum
}

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	vs.impl.trackPings = make(map[string]uint)
	vs.impl.currView = View{}
	vs.impl.seenNonZero = make(map[string]bool)
	vs.impl.primaryAcknowledged = false
	vs.impl.trackRestarted = make(map[string]bool)
	vs.impl.VSRequests = make(chan VSRequests)
	vs.impl.VSResponses = make(chan any)
	vs.impl.lastViewNum = make(map[string]uint)

	go func() {
		for fn := range vs.impl.VSRequests { // for the length of the channel having an action
			// referencing the struct's memory (the real one)
			// putting the result of the function in the response channel
			vs.impl.VSResponses <- fn(&vs.impl)
		}
	}()
}
func (vs *ViewServer) performRequests(fn VSRequests) any { // sends the request and responses between the goroutine
	// sends the function to requests channel, which is received in the goroutine to perform
	vs.impl.VSRequests <- fn
	// returns the response from the goroutine
	return <-vs.impl.VSResponses
}

// Ping() RPC handler implementation
func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) {
	pingRequests := func(*ViewServerImpl) any {
		if args.Viewnum == 0 { // ping(0) --> server is either NEW or RESTARTED
			if vs.impl.seenNonZero[args.Me] == true && vs.impl.lastViewNum[args.Me] != 0 { // it has seen numbered views, like 1, and getting 0 means it restarted
				vs.impl.trackRestarted[args.Me] = true

			} // TODO - i feel like this should only be relevant when it's active, bc if it's idle it will be restarted when we don't care
			// fixed bc i reset the Restarted Server after it's been promoted to backup or primary
		} else { // viewNum is a value. so seenNonZero is true
			vs.impl.seenNonZero[args.Me] = true
		}
		// if I received a ping, then the server is still working, so interval is at 0
		vs.impl.trackPings[args.Me] = 0
		vs.impl.lastViewNum[args.Me] = args.Viewnum
		if args.Me == vs.impl.currView.Primary && args.Viewnum == vs.impl.currView.Viewnum {
			vs.impl.primaryAcknowledged = true
		}
		reply.View = vs.impl.currView
		return nil
	}
	vs.performRequests(pingRequests)
}

// Get() RPC handler implementation
func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) {
	getRequests := func(*ViewServerImpl) any {
		reply.View = vs.impl.currView
		return nil
	}
	vs.performRequests(getRequests)
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	// when a tick is called, increase time it hasn't recieved a ping
	tickRequests := func(*ViewServerImpl) any {
		for key := range vs.impl.trackPings {
			vs.impl.trackPings[key]++
		}
		if vs.impl.currView.Primary == "" { // starting --> get initial primary
			firstPrimary := ""
			firstBackup := ""
			for key := range vs.impl.trackPings {
				if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
					vs.impl.trackPings[key] < DeadPings { // promote as a backup
					firstPrimary = key
					break
				}
			}
			if firstPrimary != "" {
				vs.impl.currView = View{
					Viewnum: vs.impl.currView.Viewnum + 1,
					Primary: firstPrimary,
					Backup:  firstBackup,
				}
				vs.impl.primaryAcknowledged = false
			}
		} else if vs.impl.currView.Primary != "" { // primary is something
			// will the primary die or restart?
			if vs.impl.trackPings[vs.impl.currView.Primary] >= DeadPings { // PRIMARY DIED, promote backup AND find a new backup
				newBackup := ""
				newPrimary := vs.impl.currView.Backup
				oldPrimary := vs.impl.currView.Primary
				// TODO if the old backup restarted during the time that primary died, then it's not a valid primary and no progress
				for key := range vs.impl.trackPings {
					if key != newPrimary && key != oldPrimary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						newBackup = key
						break
					}
				}
				if vs.impl.primaryAcknowledged { // the old primary ack the view
					vs.impl.currView = View{
						Viewnum: vs.impl.currView.Viewnum + 1,
						Primary: newPrimary,
						Backup:  newBackup,
					}
					vs.impl.primaryAcknowledged = false
					vs.impl.trackRestarted[oldPrimary] = false
					// the old primary is dead so it doens't matter, but what if it came back alive? it's basically new
					if newBackup != "" { // maybe not needed bc if it's "" it would set an "" as false
						vs.impl.trackRestarted[newBackup] = false
					}
				} else { // primary did not ack and now it's dead, so we can't make progress
					return nil
				}
			} else if vs.impl.trackRestarted[vs.impl.currView.Primary] == true { // the primary restarted
				newBackup := ""
				newPrimary := vs.impl.currView.Backup
				oldPrimary := vs.impl.currView.Primary
				// TODO if the old backup restarted during the time that primary died, then it's not a valid primary and no progress
				for key := range vs.impl.trackPings {
					if key != newPrimary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						// the restarted primary is a candidate to be the backup
						newBackup = key
						break
					}
				}
				if vs.impl.primaryAcknowledged { // the old primary ack the view
					vs.impl.currView = View{
						Viewnum: vs.impl.currView.Viewnum + 1,
						Primary: newPrimary,
						Backup:  newBackup,
					}
					vs.impl.primaryAcknowledged = false
					vs.impl.trackRestarted[oldPrimary] = false
					if newBackup != "" {
						vs.impl.trackRestarted[newBackup] = false
					}
				} else { // primary did not ack and now it's dead, so we can't make progress
					return nil
				}
			} else if vs.impl.currView.Backup == "" {
				// backup is empty --> either initial backup OR it's empty and suddendly an idle server is back on
				// primary is chill
				newBackup := ""
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						// this also means an old restarted backup is valid
						newBackup = key
						break
					}
				}
				if newBackup != "" {
					if vs.impl.primaryAcknowledged {
						vs.impl.currView = View{
							Viewnum: vs.impl.currView.Viewnum + 1,
							Primary: vs.impl.currView.Primary,
							Backup:  newBackup,
						}
						vs.impl.primaryAcknowledged = false
						vs.impl.trackRestarted[newBackup] = false
					} else {
						return nil
					}
				}
			} else if vs.impl.trackPings[vs.impl.currView.Backup] >= DeadPings { // backup dead asf
				// primary is chill
				newBackup := ""
				oldBackup := vs.impl.currView.Backup
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && key != oldBackup && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						newBackup = key
						break
					}
				}
				if vs.impl.primaryAcknowledged {
					vs.impl.currView = View{
						Viewnum: vs.impl.currView.Viewnum + 1,
						Primary: vs.impl.currView.Primary,
						Backup:  newBackup,
					}
					vs.impl.primaryAcknowledged = false
					vs.impl.trackRestarted[newBackup] = false
					vs.impl.trackRestarted[oldBackup] = false
				} else {
					return nil
				}
			} else if vs.impl.trackRestarted[vs.impl.currView.Backup] == true { // backup restarted
				// find a new backup
				newBackup := ""
				oldBackup := vs.impl.currView.Backup
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						// the new backup could be the backup that was restarted
						newBackup = key
						break
					}
				}
				if vs.impl.primaryAcknowledged {
					vs.impl.currView = View{
						Viewnum: vs.impl.currView.Viewnum + 1,
						Primary: vs.impl.currView.Primary,
						Backup:  newBackup,
					}
					vs.impl.primaryAcknowledged = false
					vs.impl.trackRestarted[newBackup] = false
					vs.impl.trackRestarted[oldBackup] = false
				} else {
					return nil
				}
			}
		}
		return nil
	}
	vs.performRequests(tickRequests)
}
