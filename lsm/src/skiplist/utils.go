package skiplist

import "math/rand"

// find the maximum height a node is placed
func MaxHeight(randGen *rand.Rand, maxHeight uint8, prob float32) uint8 {
	var height uint8 = 1
	for height < maxHeight && randGen.Float32() < prob {
		height++
	}
	return height
}
