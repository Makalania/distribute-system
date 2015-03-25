package tapestry

import (
    "testing"
    "bytes"
    "time"
  //  "fmt"
)

func TestSimple(t *testing.T) {
	SetDebug(true)
	var err error 
    var id [DIGITS]Digit

  //  id, _ = ParseID("1225")
    node1, err := Start(10000, "")
    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    id, _ = ParseID("1230")
    node2, err := start(id, 10001, node1.local.node.Address)
    if err != nil {
    	t.Errorf("Unable to create node, received error:%v\n", err)
    }

    id, _ = ParseID("1221")
    node3, err := start(id, 10002, node1.local.node.Address)
    if err != nil {
    	t.Errorf("Unable to create node, received error:%v\n", err)
    }

    id, _ = ParseID("1021")
    node4, err := start(id, 10003, node1.local.node.Address)
    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    id, _ = ParseID("1310")
    node5, err := start(id, 10004, node1.local.node.Address)
    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    node1.Leave()

    // test notify leave of root node
    // Hash("key1") is 1033, store the key/value in node2
    
    err = node2.Store("key1", []byte("value1"))
    if err != nil {
        t.Errorf("Unable to store node, received error:%v\n", err)
    }

    root, err:= node3.local.findRoot(node3.local.node, Hash("key1"))
    if err != nil {
        t.Errorf("Cant find Root, received error:%v\n", err)
    }

    rootId := root.Id
    // wait for publish
    time.Sleep(20 * time.Second)
    ans1, err := node2.Get("key1")

    if err != nil {
        t.Errorf("Unable to Get the key, received error:%v\n", err)
    } else if bytes.Compare(ans1, []byte("value1")) != 0 {
        t.Errorf("Unable to retrieve right value, received:%q\n", ans1)
    }

    // test if root leave the tapestry, would the tapestry network can still find the correct node
    if (node2.local.node.Id.big()).Cmp(rootId.big()) == 0 {
        node2.Leave();
    } else {
        if (node3.local.node.Id.big()).Cmp(rootId.big()) == 0 {
            node3.Leave();
        }
        if (node4.local.node.Id.big()).Cmp(rootId.big()) == 0 {
            node4.Leave();
        }
        if (node5.local.node.Id.big()).Cmp(rootId.big()) == 0 {
            node5.Leave();
        }

        time.Sleep(20 * time.Second)
        ans1, err = node2.Get("key1")

        if err != nil {
        t.Errorf("Unable to Get the key, received error:%v\n", err)
        } else if bytes.Compare(ans1, []byte("value1")) != 0 {
        t.Errorf("Unable to retrieve right value, received:%q\n", ans1)
        }
        
    }

    // rejoin the leave node4
    id, _ = ParseID("1021")
    node4, err = start(id, 10003, node2.local.node.Address)

    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    // store the value into node4 and leave node2 to see if we can find value that inside node3
    err = node3.Store("key1", []byte("value_replace"))
    if err != nil {
        t.Errorf("Unable to store node, received error:%v\n", err)
    }

    node2.Leave()
    // wait for publish
    time.Sleep(20 * time.Second)

    ans1, err = node4.Get("key1")
    // the value we get right now should change from "value1" to "value_replace"
    if err != nil {
        t.Errorf("Unable to Get the key, received error:%v\n", err)
    } else if bytes.Compare(ans1, []byte("value_replace")) != 0 {
        t.Errorf("Unable to retrieve right value, received:%q\n", ans1)
    }

    node4.Leave();
    ans1, err = node3.Get("key1")
    if err == nil {
        t.Errorf("Nodes contain the objects already leave, cant find the key but return value:%v\n", ans1)
    }

    // Now we only have node3(1221) and node4(1310) exists. 
    // Hash("key1") == 1033
    node3.Store("key1", []byte("value_replace"))
    //current root should be node3(1221)

    // rejoin node4 and root change to node4
    id, _ = ParseID("1021")
    node4, err = start(id, 10003, node3.local.node.Address)
    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    // we should have 2 replicas in the current root
    time.Sleep(20 * time.Second)
    replicas := node4.local.store.Get("key1")
    if replicas == nil {
        t.Errorf("Unable to retrieve key from current root store:%v\n", replicas)
    }

    // rejoin node2 and root change to node2
    id, _ = ParseID("1030")
    node2, err = start(id, 10001, node3.local.node.Address)
    if err != nil {
        t.Errorf("Unable to create node, received error:%v\n", err)
    }

    // we should have 2 replicas in the current root
    time.Sleep(20 * time.Second)
    replicas = node2.local.store.Get("key1")
    if replicas == nil {
        t.Errorf("Unable to retrieve key from current root store:%v\n", replicas)
    }

    // check lookup function
    locations, err := node2.Lookup("key1")
    if err != nil {
        t.Errorf("Unable to lookup value, received error:%v\n", err)
    } else if locations == nil {
        t.Errorf("Cant find key1 by lookup function, received:%v\n", locations)
    }


    //Test remove and leave function

    node2.Leave()
    node3.Remove("key1")
    node5.Store("key1", []byte("value1"))
    node4.Leave()
    time.Sleep(20 * time.Second)
    
    ans1, err = node5.Get("key1")
    if err != nil {
        t.Errorf("Unable to Get the key, received error:%v\n", err)
    } else if bytes.Compare(ans1, []byte("value1")) != 0 {
        t.Errorf("Unable to retrieve right value, received:%q\n", ans1)
    }
}