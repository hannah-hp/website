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
		} else { // viewNum is a value. so seenNonZero is false
			vs.impl.seenNonZero[args.Me] = true
		}
		// if I received a ping, then the server is still working, so interval is at 0
		vs.impl.trackPings[args.Me] = 0
		vs.impl.lastViewNum[args.Me] = args.Viewnum
		if args.Me == vs.impl.currView.Primary && args.Viewnum == vs.impl.currView.Viewnum {
			//fmt.Printf("ACK: server=%s view=%d\n",
			//	args.Me, args.Viewnum)
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

		// increment trackPings ++
		for key := range vs.impl.trackPings {
			vs.impl.trackPings[key]++
		}
		//fmt.Printf(
		//	"TICK: view=%+v ack=%v trackPings=%d primaryRestarted=%v \n",
		//	vs.impl.currView,
		//	vs.impl.primaryAcknowledged,
		//	vs.impl.trackPings[vs.impl.currView.Primary],
		//	vs.impl.trackRestarted[vs.impl.currView.Primary],
		//)
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
			}
		} else if vs.impl.primaryAcknowledged { // primary acknowledged viewNum, so we can do stuff
			if vs.impl.trackPings[vs.impl.currView.Primary] >= DeadPings ||
				vs.impl.trackRestarted[vs.impl.currView.Primary] == true {
				// promote backup as new primary
				// promote any available idle server as backup
				//fmt.Printf("TRACKING PRIMARY FAIL: view=%+v\n",
				//	vs.impl.currView)
				newBackup := ""
				oldPrimary := vs.impl.currView.Primary
				newPrimary := vs.impl.currView.Backup

				for key := range vs.impl.trackPings { // also allows a restarted primary to be the backup
					if key != newPrimary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						newBackup = key
						break
					}
				}
				// if no new backup was found then it's ""
				vs.impl.currView = View{
					Viewnum: vs.impl.currView.Viewnum + 1,
					Primary: newPrimary,
					Backup:  newBackup,
				}
				//fmt.Printf("TRACKING PRIMARY FAIL: view=%+v\n",
				//	vs.impl.currView)
				vs.impl.primaryAcknowledged = false        // reset acknowledgement since there's a new view
				vs.impl.trackRestarted[oldPrimary] = false // set the restarted primary to be false, bc it got booted and we dgaf
			} else if vs.impl.currView.Backup == "" { // initial backup, or find an idle server asap
				firstBackup := ""
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
						vs.impl.trackPings[key] < DeadPings { // promote as a backup
						firstBackup = key
						break
					}
				}
				if firstBackup != "" {
					vs.impl.currView = View{
						Viewnum: vs.impl.currView.Viewnum + 1,
						Primary: vs.impl.currView.Primary,
						Backup:  firstBackup,
					}
					vs.impl.primaryAcknowledged = false
				}
			} else if vs.impl.trackPings[vs.impl.currView.Backup] >= DeadPings { // dead backup
				// dead backup --> primary is fine
				// recruit idle server as backup, if none available, then it's ""
				// dead doesnot mean available, so it can't be the current backup
				newBackup := ""
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
						vs.impl.trackPings[key] < DeadPings { // promote as a backup
						newBackup = key
						break
					}
				}
				vs.impl.currView = View{ // newBackup could be idle or ""
					Viewnum: vs.impl.currView.Viewnum + 1,
					Primary: vs.impl.currView.Primary,
					Backup:  newBackup,
				}
				vs.impl.primaryAcknowledged = false
			} else if vs.impl.trackRestarted[vs.impl.currView.Backup] == true {
				// backup could be "", first available idle, or the restarted backup
				//fmt.Printf("TRACKING BACKUP: view=%+v\n",
				//	vs.impl.currView)
				//fmt.Printf(
				//	"BACKUP RESTART: view=%d backup=%s restarted=%v\n",
				//	vs.impl.currView.Viewnum,
				//	vs.impl.currView.Backup,
				//	vs.impl.trackRestarted[vs.impl.currView.Backup],
				//)
				newBackup := ""
				for key := range vs.impl.trackPings {
					if key != vs.impl.currView.Primary && vs.impl.trackPings[key] < DeadPings { // promote as a backup
						newBackup = key
						break
					}
				}
				vs.impl.currView = View{ // newBackup could be idle or ""
					Viewnum: vs.impl.currView.Viewnum + 1,
					Primary: vs.impl.currView.Primary,
					Backup:  newBackup,
				}
				vs.impl.primaryAcknowledged = false
				vs.impl.trackRestarted[vs.impl.currView.Backup] = false
				//fmt.Printf("TRACKING BACKUP: view=%+v\n",
				//	vs.impl.currView)
			}
			//}
			//////////////////////////////////////////////
			//// IF PRIMARY != "" AND BACKUP == ""
			//// so this could be initial backup
			//// or we can loop through pigns
			//if vs.impl.currView.Primary != "" && vs.impl.currView.Backup == "" { // GET INITIAL BACKUP
			//	firstBackup := ""
			//	for key := range vs.impl.trackPings {
			//		if key != vs.impl.currView.Primary && key != vs.impl.currView.Backup &&
			//			vs.impl.trackPings[key] < DeadPings { // promote as a backup
			//			firstBackup = key
			//			break
			//		}
			//	}
			//	if firstBackup != "" {
			//		vs.impl.currView = View{
			//			Viewnum: vs.impl.currView.Viewnum + 1,
			//			Primary: vs.impl.currView.Primary,
			//			Backup:  firstBackup,
			//		}
			//	}
			//}
			//
			//if vs.impl.trackRestarted[vs.impl.currView.Primary] == true ||
			//	vs.impl.trackPings[vs.impl.currView.Primary] >= DeadPings { // the primary has restarted OR primary died
			//	// promote backup as new primary
			//	// promote any available idle server as backup
			//	newBackup := ""
			//	newPrimary := vs.impl.currView.Backup
			//
			//	for key := range vs.impl.trackPings {
			//		if key != newPrimary &&
			//			vs.impl.trackPings[key] < DeadPings { // promote as a backup
			//			newBackup = key
			//			break
			//		}
			//	}
			//	// if no new backup was found then it's ""
			//	if vs.impl.primaryAcknowledged { // primary acknowledges the current view, we can upgrade
			//		vs.impl.currView = View{
			//			Viewnum: vs.impl.currView.Viewnum + 1,
			//			Primary: newPrimary,
			//			Backup:  newBackup,
			//		}
			//		vs.impl.primaryAcknowledged = false                      // reset acknowledgement since there's a new view
			//		vs.impl.trackRestarted[vs.impl.currView.Primary] = false // set the restarted primary to be false, bc it got booted and we dgaf
			//	}
			//}
			//// dead or restarted or backup
			//if vs.impl.trackPings[vs.impl.currView.Backup] >= DeadPings ||
			//	vs.impl.trackRestarted[vs.impl.currView.Backup] == true {
			//	// primary is good, we just need a new backup
			//	oldBackup := vs.impl.currView.Backup
			//	newBackup := ""
			//	for key := range vs.impl.trackPings {
			//		if key != vs.impl.currView.Primary &&
			//			vs.impl.trackPings[key] < DeadPings { // promote as a backup
			//			newBackup = key
			//			break
			//		}
			//	}
			//	if vs.impl.primaryAcknowledged {
			//		//fmt.Printf(
			//		//	"BACKUP CHANGE: view=%d old=%s new=%s restarted=%v ack=%v\n",
			//		//	vs.impl.currView.Viewnum,
			//		//	oldBackup,
			//		//	newBackup,
			//		//	vs.impl.trackRestarted[oldBackup],
			//		//	vs.impl.primaryAcknowledged,
			//		//)
			//		// no idle server, so backup is blank
			//		if newBackup == "" && newBackup != oldBackup {
			//			vs.impl.currView = View{
			//				Viewnum: vs.impl.currView.Viewnum + 1,
			//				Primary: vs.impl.currView.Primary, // primary stayed the same
			//				Backup:  newBackup,                // empty
			//			}
			//		} else if newBackup != "" && vs.impl.trackRestarted[vs.impl.currView.Backup] == true { // newBackup is either an idle server, or the same backup that was restarted
			//			//fmt.Printf(
			//			//	"i should be here : view=%d old=%s new=%s restarted=%v ack=%v\n",
			//			//	vs.impl.currView.Viewnum,
			//			//	oldBackup,
			//			//	newBackup,
			//			//	vs.impl.trackRestarted[oldBackup],
			//			//	vs.impl.primaryAcknowledged,
			//			//)
			//			vs.impl.currView = View{
			//				Viewnum: vs.impl.currView.Viewnum + 1,
			//				Primary: vs.impl.currView.Primary, // primary stayed the same
			//				Backup:  newBackup,
			//			}
			//			vs.impl.trackRestarted[vs.impl.currView.Backup] = false // set the restarted primary to be false, bc it got booted and we dgaf
			//		}
			//		vs.impl.primaryAcknowledged = false // reset acknowledgement since there's a new view
			//
			//	}
			//
		}
		return nil
	}
	vs.performRequests(tickRequests)
}
