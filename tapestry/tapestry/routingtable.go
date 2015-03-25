package tapestry

import (
	"sync"
)

/*
	A routing table has a number of levels equal to the number of digits in an ID (default 40)

	Each level has a number of slots equal to the digit base (default 16)

	A node that exists on level n thereby shares a prefix of length n with the local node.

	Access to the routing table is managed by a lock
*/
type RoutingTable struct {
	local Node                  // the local tapestry node
	mutex sync.Mutex            // to manage concurrent access to the routing table. could have a per-level mutex though
	rows  [DIGITS][BASE]*[]Node // the rows of the routing table
}

/*
	Creates and returns a new routing table, placing the local node at the appropriate slot in each level of the table
*/
func NewRoutingTable(me Node) *RoutingTable {
	t := new(RoutingTable)
	t.local = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			slot := make([]Node, 0, SLOTSIZE)
			t.rows[i][j] = &slot
		}
	}

	// Make sure each row has at least our node in it
	Debug.Printf("Ini node %v's routing table \n", me)
	for i := 0; i < DIGITS; i++ {
		slot := t.rows[i][t.local.Id[i]]
		*slot = append(*slot, t.local)
	}

	return t
}

/*
	Adds the given node to the routing table

	Returns true if the node did not previously exist in the table and was subsequently added

	Returns the previous node in the table, if one was overwritten
*/
func (t *RoutingTable) Add(node Node) (added bool, previous *Node) {
	t.mutex.Lock()
	// TODO: Students should implement this
	//Debug.Println("Add get the lock")
	//Debug.Printf("%v's routing table will add node %v\n", t.local, node)
	id1 := node.Id	
	id2 := t.local.Id
	PreLen := SharedPrefixLength(id1, id2)
	if PreLen == DIGITS {
		t.mutex.Unlock()
		return false, nil
	}
	col := id1[PreLen]

	slot := t.rows[PreLen][col]
	added = true
	for i := 0; i < len(*slot); i++ {
		if (((*slot)[i].Id).big()).Cmp(id1.big()) == 0 {
			added = false
		} 
	}

	if added == true {
		if len(*slot) <= 2 {
			*slot = append(*slot, node)

			// sort slot 
			for i := 0; i < (len(*slot) - 1); i++ {
				for j := i + 1; j < len(*slot); j++ {
					first := (*slot)[i]
					second := (*slot)[j]
					change := id2.Closer(first.Id, second.Id)
					if change == false {
						(*slot)[i] = second
						(*slot)[j] = first
					}
				}
			}
		} else {
			insert := id2.Closer(id1, (*slot)[2].Id)
			if insert {
				previous = &((*slot)[2])
				(*slot)[2] = node
				// sort slot
				for i := 0; i < (len(*slot) - 1); i++ {
					for j := i + 1; j < len(*slot); j++ {
						first := (*slot)[i]
						second := (*slot)[j]
						change := id2.Closer(first.Id, second.Id)
						if change == false {
							(*slot)[i] = second
							(*slot)[j] = first
						}
					}
				}	
			} else {
				added = false
			}
		}
	}

	t.mutex.Unlock()
	//Debug.Println("Add return the lock")
	return added, previous
}

/*
	Removes the specified node from the routing table, if it exists

	Returns true if the node was in the table and was successfully removed
*/
func (t *RoutingTable) Remove(node Node) (wasRemoved bool) {
	t.mutex.Lock()
	// TODO: Students should implement this
	NodeId := node.Id
	PreLen := SharedPrefixLength(NodeId, t.local.Id)
	col := NodeId[PreLen]
	slot := t.rows[PreLen][col]
	wasRemoved = false
	for j := 0; j < len(*slot); j++ {
		compare := (*slot)[j].Id.big()
		
		// if found node to remove
		if compare.Cmp(NodeId.big()) == 0 {
			if len(*slot) == 1 {
				iniSlot := make([]Node, 0, SLOTSIZE)
			    t.rows[PreLen][col] = &iniSlot
			    break
			}
			for i := j; i < len(*slot) - 1; i++ {
				(*slot)[i] = (*slot)[i + 1]
			}
			newSlot := (*slot)[:len(*slot) - 1]
			t.rows[PreLen][col] = &newSlot
			wasRemoved = true
			break
		}
	}
	
	t.mutex.Unlock()
	return wasRemoved
}

/*
	Get all nodes on the specified level of the routing table, EXCLUDING the local node
*/
func (t *RoutingTable) GetLevel(level int) (nodes []Node) {
	t.mutex.Lock()
	// TODO: Students should implement this
	// Debug.Printf("start to get leve %v\n", level)
	for i := 0; i < BASE; i++ {
		slot := t.rows[level][i]
		if len(*slot) != 0 && ((*slot)[0].Id.big()).Cmp(t.local.Id.big()) != 0 {
			nodes = append(nodes, (*slot)[0])
		}				
	}

	t.mutex.Unlock()
	return nodes
}

/*
	Search the table for the closest next-hop node for the provided ID
*/
func (t *RoutingTable) GetNextHop(id ID) (node Node) {
	t.mutex.Lock()
	// TODO: Students should implement this
	Debug.Printf("node %v's routing table ", t.local)
	localId :=  t.local.Id	
	n := SharedPrefixLength(localId, id)
	res := t.local
	for n < DIGITS {
		//Debug.Printf("one iteration,leve %v", n)

		// Scan all the nodes of this level
		for i := 0; i < BASE; i++ {
			slot := t.rows[n][i]
			if len(*slot) != 0 {
				cur := (*slot)[0]
				if id.BetterChoice(cur.Id, res.Id) {
					res = cur 
				}
			}		
		}

		// If the selected node is local node selft, 
		// continue to scan the next level until the last
		// level
		if res.Id.big().Cmp(t.local.Id.big()) == 0 {
			n++
		} else {
			break
		}
	} 

	t.mutex.Unlock()
	return res
}
