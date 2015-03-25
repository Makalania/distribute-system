package tapestry

import (
	"fmt"
	"time"
)

/*
   Implementation of the local tapestry node.  There are three kinds of methods defined in this file

       1.  Methods that can be invoked remotely via RPC by other Tapestry nodes (eg AddBackpointer, RemoveBackpointer)
       2.  Methods that are invoked by clients (eg Publish, Lookup)
       3.  Common utility methods that you can use in your implementation (eg findRoot)

   For RPC methods, to invoke the equivalent method on a remote node, see the methods defined in tapestry-remote.go
*/

/*
   The ID and location information for a node in the tapestry
*/
type Node struct {
	Id      ID
	Address string
}

/*
   The main struct for the local tapestry node.

   Methods can be invoked locally on this struct.

   Methods on remote TapestryNode instances can be called on the 'tapestry' member
*/
type TapestryNode struct {
	node         Node          // the ID and address of this node
	table        *RoutingTable // the routing table
	backpointers *Backpointers // backpointers to keep track of other nodes that point to us
	store        *ObjectStore  // stores keys for which this node is the root
	tapestry     *Tapestry     // api to the tapestry mesh
}

/*
   Called in tapestry initialization to create a tapestry node struct
*/
func newTapestryNode(node Node, tapestry *Tapestry) *TapestryNode {
	n := new(TapestryNode)
	n.node = node
	n.table = NewRoutingTable(node)
	n.backpointers = NewBackpointers(node)
	n.store = NewObjectStore()
	n.tapestry = tapestry
	return n
}

/*
   Invoked when starting the local node, if we are connecting to an existing Tapestry.

   *    Find the root for our node's ID
   *    Call AddNode on our root to initiate the multicast and receive our initial neighbour set
   *    Iteratively get backpointers from the neighbour set and populate routing table
*/
func (local *TapestryNode) Join(otherNode Node) error {
	// Route to our root
	Debug.Printf("Joining %v\n", otherNode)
	root, err := local.findRoot(otherNode, local.node.Id)
	if err != nil {
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
	}

	// Add ourselves to our root by invoking AddNode on the remote node
	Debug.Printf("start adding nodes: %v \n", local.node)
	neighbours, err := local.tapestry.addNode(root, local.node)
	if err != nil {
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
	}

	// Add the neighbours to our local routing table.
	for _, n := range neighbours {
		local.addRoute(n)
	}
	Debug.Println("now start to backpointer traversal");
	// TODO: Students should implement the backpointer traversal portion of Join

	// Use set to avoid duplicates
	nextNeighbours := NewNodeSet();
	// Initialized to the neighbours
	nextNeighbours.AddAll(neighbours)

	startLevel := SharedPrefixLength(root.Id, local.node.Id)
	return local.traverseBackpointers(nextNeighbours, startLevel)
}

/*
   Client API function to gracefully exit the Tapestry mesh

   *    Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
   *    If possible, give each backpointer a suitable alternative node from our routing table
*/
func (local *TapestryNode) Leave() (err error) {

	// TODO: Students should implement this
	for i := 0; i < DIGITS; i++ {
		backpointers := local.backpointers.Get(i);

		for _, backpointer := range backpointers {
			
			// Find a alternative node
			var replacement *Node
			for j := i + 1; j < DIGITS; j++ {
				nodes := local.table.GetLevel(j);
				if nodes != nil {
					replacement = &nodes[0];
					break;
				}
			}

			// Notify the backpointer that we are leaving
			err = local.tapestry.notifyLeave(backpointer, local.node, replacement);
			if err != nil {
				return err;
			}
		}
	}

	return nil;
}

/*
   Client API.  Publishes the key to tapestry.

   *    Route to the root node for the key
   *    Register our local node on the root
   *    Start periodically republishing the key
   *    Return a channel for cancelling the publish
*/
func (local *TapestryNode) Publish(key string) (done chan bool, err error) {
	// TODO: Students should implement this

	ticker := time.NewTicker(REPUBLISH);
	done = make(chan bool)
    	go func() { 
	    for {
	       select {
	        case <- ticker.C:
	        	node, err := local.findRoot(local.node, Hash(key));
			if err != nil {
				continue;
			}
			_, err = local.tapestry.register(node, local.node, key);
			if err != nil {
				continue;
			}
	        case <- done:
	            	ticker.Stop();
                    	return
	        }
	    }
	}()

	return done, nil
}

/*
   Client API.  Look up the Tapestry nodes that are storing the blob for the specified key.

   *    Find the root node for the key
   *    Fetch the replicas from the root's object store
   *    Attempt up to RETRIES many times
*/
func (local *TapestryNode) Lookup(key string) (nodes []Node, err error) {
	// TODO: Students should implement this
	node, err := local.findRoot(local.node, Hash(key));
	if err != nil {
		return nil, err;
	}
	for i := 0; i < RETRIES; i++ {
		_, nodes, err = local.tapestry.fetch(node, key);
		if(err == nil) {
			return nodes, nil;
		}
	}
	return nil, err
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   We are the root node for some new node joining the tapestry.
   *    Begin the acknowledged multicast
   *    Return the neighbourset from the multicast
*/
func (local *TapestryNode) AddNode(node Node) (neighbourset []Node, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.Id, local.node.Id))
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   A new node is joining the tapestry, and we are a need-to-know node participating in the multicast.
   *    Propagate the multicast to the specified row in our routing table
   *    Await multicast response and return the neighbourset
   *    Begin transfer of appropriate replica info to the new node
*/
func (local *TapestryNode) AddNodeMulticast(newnode Node, level int) (neighbours []Node, err error) {
	Debug.Printf("Add node multicast %v at level %v\n", newnode, level)
	// TODO: Students should implement this

	var results []Node
	if level >= DIGITS {
		return 	results, nil
	}

	targets := local.table.GetLevel(level);

	// local node must participate in
	targets = append(targets, local.node)
	for _, target := range targets {
		nexts, err := local.tapestry.addNodeMulticast(target, newnode, level + 1);
		if err != nil {
			return nil, err 
		}

		for _, next := range nexts {
			results = append(results, next);
		}
	}

	// Begin transfer of appropriate replica info to the new node
	data := local.store.GetTransferRegistrations(local.node, newnode)
	err = local.tapestry.transfer(newnode, local.node, data)

	if(err != nil) {
		return nil, err;
	}

	for _, target := range targets {
		results = append(results, target);
	}

	return results, err;
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   Another node is informing us of a graceful exit.
   *    Remove references to the from node from our routing table and backpointers
   *    If replacement is not nil, add replacement to our routing table
*/
func (local *TapestryNode) NotifyLeave(from Node, replacement *Node) (err error) {
	Debug.Printf("Received leave notification from %v with replacement node %v\n", from, replacement)

	// TODO: Students should implement this

	// Remove the from node from our routing table
	if local.table.Remove(from) {
		Debug.Printf("Removed the node %v\n from our routing table", from);
	}
    
    // Remove the from node from our backpointers
	local.RemoveBackpointer(from) 
		
	if replacement != nil {
		err := local.addRoute(*replacement);
		if err != nil {
			return err
 		}
	}

	return nil;
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Returns the best candidate from our routing table for routing to the provided ID
*/
func (local *TapestryNode) GetNextHop(id ID) (morehops bool, nexthop Node, err error) {
	// TODO: Students should implement this

	nexthop = local.table.GetNextHop(id)
	if local.node.Id.big().Cmp(nexthop.Id.big()) == 0 {
		return false, nexthop, nil
	}
	return true, nexthop, nil
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Add the from node to our backpointers
   *    Possibly add the node to our routing table, if appropriate
*/
func (local *TapestryNode) AddBackpointer(from Node) (err error) {
	Debug.Printf("add back pointers%v\n", from)
	if local.backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.addRoute(from)
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Remove the from node from our backpointers
*/
func (local *TapestryNode) RemoveBackpointer(from Node) (err error) {
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Get all backpointers at the level specified
   *    Possibly add the node to our routing table, if appropriate
*/
func (local *TapestryNode) GetBackpointers(from Node, level int) (backpointers []Node, err error) {
	if level < 0 {
		return 
	}
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.backpointers.Get(level)
	local.addRoute(from)
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   The provided nodes are bad and we should discard them
   *    Remove each node from our routing table
   *    Remove each node from our set of backpointers
*/
func (local *TapestryNode) RemoveBadNodes(badnodes []Node) (err error) {
	for _, badnode := range badnodes {
		if local.table.Remove(badnode) {
			Debug.Printf("Removed bad node %v\n", badnode)
		}
		if local.backpointers.Remove(badnode) {
			Debug.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.
   Register the specified node as an advertiser of the specified key.

   *    Check that we are the root node for the key
   *    Add the node to the object store
   *    Kick off a timer to remove the node if it's not advertised again after a set amount of time
*/
func (local *TapestryNode) Register(key string, replica Node) (isRoot bool, err error) {
	// TODO: Students should implement this
	node, err := local.findRoot(local.node, Hash(key));
	if err != nil {
		return false, err
	}

	isRoot = ((node.Id.big()).Cmp(local.node.Id.big()) == 0)
	if isRoot {
		local.store.Register(key, replica, TIMEOUT);
	}

	return isRoot, err;
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Check that we are the root node for the requested key
   *    Return all nodes that are registered in the local object store for this key
*/
func (local *TapestryNode) Fetch(key string) (isRoot bool, replicas []Node, err error) {
	// TODO: Students should implement this
	_, isRoot = local.store.data[key];
	if isRoot {
		return isRoot, local.store.Get(key), nil 
	}
	return isRoot, nil, nil
}

/*
   This method is invoked over RPC by other Tapestry nodes

   *    Register all of the provided objects in the local object store
   *    If appropriate, add the from node to our local routing table
*/
func (local *TapestryNode) Transfer(from Node, replicamap map[string][]Node) error {
	// TODO: Students should implement this

	local.store.RegisterAll(replicamap, TIMEOUT);
	err := local.addRoute(from);

	return err
}

/*
   Utility function that adds a node to our routing table

   *    Adds the provided node to the routing table, if appropriate.
   *    If the node was added to the routing table, notify the node of a backpointer
   *    If an old node was removed from the routing table, notify the old node of a removed backpointer
*/
func (local *TapestryNode) addRoute(node Node) (err error) {
	// TODO: Students should implement this

	Debug.Printf("will add node %v to node %v's route \v", node, local.node)
	added, previous := local.table.Add(node);

	if added {
		err := local.tapestry.addBackpointer(node, local.node);
		if err != nil {
			return err;
		}
	}

	if previous != nil {
		err := local.tapestry.removeBackpointer(*previous, local.node);
		if err != nil {
			return err;
		}
	}

	return nil;
}

/*
   Utility function for iteratively contacting nodes to get the root node for the provided ID

   *    Starting from the specified node, iteratively contact nodes calling getNextHop until we reach the root node
   *    Also keep track of any bad nodes that errored during lookup
   *    At each step, notify the next-hop node of all of the bad nodes we have encountered along the way
*/
func (local *TapestryNode) findRoot(start Node, id ID) (Node, error) {
	Debug.Printf("Routing to %v\n", id)
	// TODO: Students should implement this
	
	var badNodes []Node;
	next := start;
	var cur Node
	pre := start
	var err error
	morehops := true;
	for morehops {
		cur = next

		// Remove the bad nodes of current node
		if len(badNodes) != 0 {
			local.tapestry.removeBadNodes(cur, badNodes);
		}

		morehops, next, err = local.tapestry.getNextHop(cur, id);
		if err != nil {
			// Keep track of bad nodes
			badNodes = append(badNodes, cur);
			// If run into bad nodes, go to the previous hop to contiue
			next = pre;
		} else {
			pre = cur;
		}
	}
	Debug.Printf("Succeed to find %v's root: %v \n", id, cur)
	return cur, nil
}

func (local *TapestryNode) traverseBackpointers(neighbours *NodeSet, level int) (err error) {
	
	// First, add all the neighours int this level to our routing table
	//Debug.Printf("Start to traverse %v's backpointers from neighbours %v \n", local.node, neighbours)
	for neighbour := range neighbours.data {
		err := local.addRoute(neighbour);
		if err != nil {
			return err;
		}
	}

	if level >= 0 {

		//nextNeighbours should not be initilized to empty
		nextNeighbours := neighbours

		for neighbour := range neighbours.data {

			backpointers, err := local.tapestry.getBackpointers(neighbour, local.node, level)
				
			if err != nil {
				return err
			}

			nextNeighbours.AddAll(backpointers)
				
			if nextNeighbours.Size() > K {
				temp := nextNeighbours.Nodes()[0 : K]
				nextNeighbours = NewNodeSet();
				nextNeighbours.AddAll(temp)
			}
		}

		err := local.traverseBackpointers(nextNeighbours, level - 1);
		if err != nil {
			return err;
		}
	}

	return nil;
}
