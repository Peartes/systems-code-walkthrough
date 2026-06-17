package skiplist

import (
	"math/rand"
)

// A Node represent a data in the skiplist
// nodes have a key and it value
// a tombstone marker to represent a deleted node
// a height array of nodes that stores pointers to the next node at each level
type Node struct {
	key       string
	value     []byte
	tombstone bool
	next      []*Node // length of array is the max height this node is on
}

// Skiplist is a sorted, efficient data structure that offers efficient
// lookups and insert. Skiplist are similar to linked list but with clever
// way to structure data that allows us to skip some of the data in the list during
// lookups. The way to acheive this is to achieve the linked list on different levels
// where a level is a linked list with some or all of the data
// ofcourse for this to work, we make sure that the number of data in higher level get
// lower each time to make it more sparse as we go higher so that we can skip more as we go higher
// this system gives us a O(lg n) lookup time
// to lookup, we keep a reference to a sentinel node unlike skip list
// this sentinel node exists on all levels and has values infintely small although this does not matter
// because it's value is never read. The lookup algorithm looks ahead (forward linked list) and never at the current
// or previous node. A skiplist has a maximum height that matters because the heigher the list determines the amount
// of data it can efficiently handle before it's performance starts to degrade. Think about it, the less the level the more
// data are on that level, the more comparison, the worse the time for lookup. A good number is 16 with a probability of 0.5
// the probability determines if a node goes to the next level.
type SkipList struct {
	root      *Node      // reference to the sentinel node
	rand      *rand.Rand // reference to a random numnber generator passed to the maxHeight() function
	maxHeight int        // the maximum height of this skip list
	prob      float32    // the probability used to get a node max height 0 - 1
	length    int        // the number of nodes in the skiplist
}

// create a new skiplist structure
func NewSkipList(maxHeight int, prob float32, rand *rand.Rand) SkipList {
	// create the sentinel node
	sentinel := &Node{
		key:       "",
		value:     nil,
		tombstone: false,
		next:      make([]*Node, maxHeight),
	}
	return SkipList{
		root:      sentinel,
		rand:      rand,
		maxHeight: maxHeight,
		prob:      prob,
	}
}

// Insert adds a new node to the skiplist at a position to keep the order of the list
// it overwrites already existing nodes and acts as an update operation that way
// to insert we first lookup to get the index of the greatest key that's less than the
// key to be inserted or if we find the key itself
func (sk *SkipList) Insert(key string, value []byte, tombstone bool) (oldValueLen int, wasOverwrite bool) {
	// prevNode is the previous node if found is false and the node itself if found is true
	prevNode, update, found := sk.Lookup(key)
	if found {
		size := len(prevNode.value)
		// this key exist, update it
		prevNode.value = value
		prevNode.tombstone = tombstone
		return size, true
	}
	// get the height of this node
	height := MaxHeight(sk.rand, uint8(sk.maxHeight), sk.prob)
	node := &Node{
		key:       key,
		value:     value,
		tombstone: tombstone,
		next:      make([]*Node, height),
	}
	// the prevNode returned is on the first level so we walk back up using the update array
	// to update all nodes in every level above that we walked
	update[0] = prevNode
	for i := 0; i < int(height); i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}
	sk.length++
	return 0, false
}

// Lookup searches for a node in a skip list and performs a partial search for insertion if the partial boolean is set
// Search begins at the highest level with the sentinel node
// we then look forward at the next node pointed to and see if the keys match
// the key we're looking for and if it does, we return the next node
// otherwise, if the forward node key is smaller than the  search key, we move right to the node and repeat
// if the forward node key is greater than the search key, we go one level down and repeat
// if this is a partial seatch (a search for the location to insert a node), then we update the update array
// we then keep repeating until we either find the node or hit a nil key signifying the end of the skiplist
// if we found the node, we set the last return value to true and we return the current node regardless
func (sk *SkipList) Lookup(key string) (*Node, []*Node, bool) {
	// get the root node which is the sentinel node
	curNode := sk.root
	// list of nodes to update at different heights
	update := make([]*Node, sk.maxHeight)
	// start from the heighest level and look forward to the next node
	height := len(curNode.next) - 1 // we start the topmost level
	for {
		nextNode := curNode.next[height]
		if nextNode == nil {
			// we've reached the end of the list at this height
			// we see if we can go lower and do so otherwise we end the lookup
			if height == 0 {
				// we are at the lowest level
				return curNode, update, false
			} else {
				// update the list of the current node before descending in order to update those height during an insert
				update[height] = curNode
				height--
				continue
			}
		}
		if nextNode.key == key {
			// we have found the node
			return nextNode, update, true
		} else if nextNode.key < key {
			// we move to the right of the list
			curNode = nextNode
			continue
		} else {
			// if this is a partial search we check if we're on the last level
			// if so, we've found the largest key smaller than the search key
			if height == 0 {
				return curNode, update, false
			}
			// the next node key is larger so we move down a level and repeat
			update[height] = curNode
			height--
			continue
		}
	}
}

// Get is a wrapper around Insert to avoid returning
// all pointer to the nodes on all level of a skiplist
func (sk *SkipList) Get(key string) (value []byte, tombstone bool, found bool) {
	node, _, found := sk.Lookup(key)
	if !found {
		return nil, false, found
	}
	return node.value, node.tombstone, true
}

// Iterate walks the skiplist on the first level applying the func to each node
// in the skiplist and stops early if fn returns false
func (sk *SkipList) Iterate(fn func(key string, value []byte, tombstone bool) bool) bool {
	currNode := sk.root
	for {
		nextNode := currNode.next[0]
		if nextNode == nil {
			break
		}
		if !fn(nextNode.key, nextNode.value, nextNode.tombstone) {
			return false
		}
		currNode = nextNode
	}
	return true
}

// Len gets the number of entries in the skip list
func (sk *SkipList) Len() int {
	return sk.length
}
