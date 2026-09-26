package kvstore

// A request is a function shipped to the goroutine that owns the integer.
type RWRequests func(* map[string]string) any
type cacheRequests func(*map[string]int) any

// additions to KVServer state.
type KVImpl struct {
	cache map[string]int // map to store client & seqNum
	memStorage map[string]string // map to store key/values

	RWRequests chan RWRequests // channel for requesting from read/writes
	RWResponses chan any // channel for read/writes responses

	cacheRequests chan cacheRequests // channel for requesting from op cache
	cacheResponses chan any // channel for op cache responses
}

func (kvs *KVServer) initKVImpl() {
	
	// initalize cache and memory
	kvs.impl.cache = make(map[string]int) // operation cache client: seqNum
	kvs.impl.memStorage = make(map[string]string) // key:value map

	// making the channel when server begins
	kvs.impl.RWRequests = make(chan RWRequests)
	kvs.impl.RWResponses = make(chan any)
	kvs.impl.cacheRequests = make(chan cacheRequests)
	kvs.impl.cacheResponses = make(chan any)

	// This assignment restricts the direction of requests/reponses to
	// the outside world
	// KVState.requests = requests
	// KVState.responses = responses

	// begin goroutines to always run
	go kvs.operationCache() 
	go kvs.readWrite()
}
func (kvs *KVServer) readWrite() {
	for fn := range kvs.impl.RWRequests {
		kvs.impl.RWResponses <- fn(&kvs.impl.memStorage) 
	}
}

func (kvs *KVServer) operationCache() {
	for fn := range kvs.impl.cacheRequests {
		kvs.impl.cacheResponses <- fn(&kvs.impl.cache) 
	}
}
	
func (kvs *KVServer) performCache(fn cacheRequests) any { // sends the request and responses between the goroutine
	// sends the function to requests channel, which is received in the goroutine to perform 
	kvs.impl.cacheRequests <- fn 
	return <-kvs.impl.cacheResponses
}

func (kvs *KVServer) performRW(fn RWRequests) any { // sends the request and responses between the goroutine
	// sends the function to requests channel, which is received in the goroutine to perform 
	kvs.impl.RWRequests <- fn 
	return <-kvs.impl.RWResponses
}

func (kvs *KVServer) Operation(args *OpArgs, reply *OpReply) error {
	if args.Op == "Get" {
		fn := func(memStorage *map[string]string) any { // get value in memStorage
			value := (*memStorage)[args.Key] // htis is what i want it to do. pointing to the isntance, but not the actual value
			return value
		}
		// calls performRW on the fn 
		// performRW will load the request channel for the goroutine to do work
		value := kvs.performRW(fn).(string) // returning a string type
		if value != "" { // value was successfully retrieved
			reply.Value = value
			reply.Err = OK
		} else { // no value, so there is an error
			reply.Err = ErrNoKey  
		}

		return nil // must return something, but since reply does, we return nil
	}
	if args.Op == "Put" {
		hasSeen := false
		cachefn := func(cache *map[string]int) any {
			// check with operation cache if seqNum appears
			// if not, call on performRW
			if client, ok := (*cache)[args.Client]; ok && client == args.SeqNo { 
				// if client and seqNum are in the operation cache, we've seen the operation and it has been done
				// just return the reply
				hasSeen = true
				reply.Err = OK
			} else { // client & seqNum not in the cache, must put in the cache & perform the request
				(*cache)[args.Client] = args.SeqNo // put client and seqNum in cache
				hasSeen = false
			}	
			// TODO : MISSING RETURN
		}
		if !hasSeen { // client & seqNum not in the cache, must put in the cache & perform the request
			// pass the func of putting client:seqNo in the cache
			kvs.performCache(cachefn).(string) 

			// create the function to add key:value in the map
			readwritefn := func(memStorage *map[string]string) any {
				(*memStorage)[args.Key] = args.Value
				// TODO : maybe no return bc the RW already returns something?
			}
			// pass the key:value func 
			kvs.performRW(readwritefn).(string)
		}
		return nil
	}
// 	type OpReply struct {
// 	Err   Err    // One of the Err codes
// 	Value string // value of key (Get only)
// }

	// if args.Op == "Put" {
	// 	fn := func(cache *map[string]int) any {

	// 		/*
	// 		maybe, 
	// 		check if client & seqNo is in the opCache
	// 		PUT
	// 		like, return a bool instead.
	// 		if (false) // not in the cache, so put it in then request 
	// 			*cache[args.Client] = args.SeqNo
	// 			// performRW but to write specifically 
	// 			(*memStorage)[args.Key] = args.value
	// 			return *memStorage ???

	// 		else // true, client and seqNo in op cache, so just ask to read again
	// 			value := (*memStorage)[args.Key]

	// 		outside of if/else 
	// 		return kvs.performRW(fn).(string)

	// 		APPEND 

	// 		if (false) // not in the cache, so put it in then request 
	// 			*cache[args.Client] = args.SeqNo
	// 			// performRW but to write specifically 
	// 			(*memStorage)[args.Key] = (*memStorage)[args.Key] + args.value
	// 			// assuming it works sequentially
	// 			access diskWrite(args.Key, (*memStorage)[args.Key])

	// 			return *memStorage ???

	// 		else // true, client and seqNo in op cache, so just ask to read again
	// 			value := (*memStorage)[args.Key]

	// 		outside of if/else 
	// 		return kvs.performRW(fn).(string)

	// 		*/
	// 		if args.Client, ok := (*cache)[args.Client]; ok && val == args.SeqNo { 
	// 			// if the client AND seqNo are in the operation cache, then the operation has been seen
	// 			// so now we want to access the ReadWrite() to get the value
	// 			// send a request to the performRW()
	// 			value := (*memStorage)[args.Key] // htis is what i want it to do. pointing to the isntance, but not the actual value
	// 			return value
	// 		} else {
	// 			 // put the seqNo in the operation cache, indicate that we've seen the operation
	// 			*cache[args.Client] = args.SeqNo
	// 			// put the key and value into memStorage --> perform the PUT request
	// 			(*memStorage)[args.Key] = args.Value
	// 		}
	// 	}
	// }
	// if args.Op == "Append" {}
	return nil
}


// ch <- value --> send value into channel ch
// value := <- ch --> receive something from channel ch and store it into value