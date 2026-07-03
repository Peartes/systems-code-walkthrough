// this is the bloom filter package for the LSM tree
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

const h1Seed = 0xDEADBEEF // hash 1 seed
const h2Seed = 0xCAFEBABE // hash 2 seed

type Bloom struct {
	n     int     // number of keys we are to insert into this bloom filter
	fpr   float64 // the desired false positive rate
	m     int     // the number of bits in our bloom filter
	k     int     // the number of bits each key is to set in the filter - doubles for the number of hash function we need
	table []uint8 // our bit-set array. each element in the array is 8bit of the bloom filter
}

// New creates a new bloom filter optimizing the number of bits allocated based on
// the number of items (keys) to be inserted and the false positive rate
//
// A bloom filter is a very interesting data structure that can tell us if a key was probably inserted
// into memory and definitively if it was not
//
// fpr is the false positive rate. The number of times the bloom filter tells us that a key was set
// but the key wasn't. To achieve a certain FPR we need to select our number of bits appropriately
//
// For some context. We set a bit on if the hash of a key maps to that bit using hash(key) % m
//
// Now with all hash function there can be collisions also, some hash will map to the same bit.
// This means there will be some false positives. To reduce the false positives we can increase m but that
// significantly increases the space required to store the keys. Another more elegant way is to require that
// a key sets more than one bit. A key has to set multiple bits and is considered to be in a SSTable if all
// the bits are set. This could reduce the fpr because each key now has to set multiple bits
// but as you'd imagine it also means we set more bit and that takes the fpr up lol so we need to
// change the m - increase it
//
// bloom filters have 2 free parameters (m, k) but really only one independent choice
//
// to choose your paramters, first you have your n and desired fpr then you find the m - number of bits in the bloom filter
// m = -n * ln(p) / ln(2)^2
//
// once you have the m then you find the k; k = m/n * ln(2)
// you can read more or research the math behind it to understand the math more
func New(n int, fpr float64) (*Bloom, error) {
	m := int(math.Ceil(-float64(n) * math.Log(float64(fpr)) / math.Pow(math.Ln2, 2)))
	// let's make m a multiple of 8bits/1byte. we do this to not have to pay for wasted bits
	// Computers can't physically store 73 bits. The smallest allocation unit is a byte (8 bits). So:
	// If you want m = 73 bits, you allocate ⌈73/8⌉ = 10 bytes = 80 bits of physical storage.
	// The last 7 bits exist, but you never touch them because your insert/query does slot = hash % 73. Those 7 bits stay 0 forever.
	if m <= 0 {
		return nil, fmt.Errorf("%w", ErrInvalidBitLength)
	}
	if m%8 != 0 {
		// m is not a multiple of 8
		m = m + (8 - m%8)
	}
	// now let's get the k
	k := int(math.Ceil(float64(m/n) * math.Ln2))

	if k <= 0 {
		return nil, fmt.Errorf("%w", ErrInvalidHashNumber)
	}

	return &Bloom{
		n:     n,
		fpr:   fpr,
		m:     m,
		k:     k,
		table: make([]uint8, m/8),
	}, nil
}

// Add adds a key to a bloom filter by setting all the bits the key maps to on
// To add keys to a bloom filter, we need to set all k bits on. Each bit is set by a separate
// hash function. Rather than using mukltiple hash functions, we use the KM single hash technique
// this allows use one hash function to derive any number of hash using the linear combination
// h(i) = (h(x)+i*h(y)) mod m where m is the size of the hash table in our case the number of bits in our bloom filter
func (blm *Bloom) Add(key string) error {
	// for each bit that needs to be set, we need the h(i) hash
	hash1, err := hashWithSeed(key, h1Seed)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAddingKey, err)
	}
	hash2, err := hashWithSeed(key, h2Seed)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAddingKey, err)
	}
	for i := range blm.k {
		// now generate the h(i) hash
		hash := (hash1 + uint64(i)*hash2) % uint64(blm.m)
		// get the position to flip
		pos, bit := blm.getBloomFilterPos(hash)
		blm.table[pos] |= (1 << bit)
	}
	return nil
}

// hashWithSeed hashes a key with a hash derived from the seed provided
// and a base hash function - FNV hash function
func hashWithSeed(key string, seed uint32) (uint64, error) {
	// first derive the hash function from the fnv hash using the see
	fnvHash := fnv.New64()
	// prepare the seed
	seedBz := make([]byte, 4)
	binary.LittleEndian.PutUint32(seedBz, seed)
	// write the seed out into the hash
	_, err := fnvHash.Write(seedBz)
	if err != nil {
		return 0, fmt.Errorf("error writing out key into hash %d, %w", seed, ErrHashingGeneration)
	}
	// write the key into the hash
	_, err = fnvHash.Write([]byte(key))
	if err != nil {
		return 0, fmt.Errorf("error writing out key into hash %d, %w", seed, ErrHashingGeneration)
	}
	// compile the hash
	return fnvHash.Sum64(), nil
}

// getBloomFilterPos is a helper method to get the position of a jey in a bloom filter
// returning the position of the bit in the bit array and the bit itself
func (blm *Bloom) getBloomFilterPos(hash uint64) (int, int) {
	// now set the bit at position hash to 1
	// let's get the position of this bit in the bit array
	pos := hash / 8
	// and the position of the bit at that word
	bit := hash % 8
	return int(pos), int(bit)
}

// MayContain tells us if a key was inserted into a bloom filter
// a positive response does not mean the key was definitively inserted but that it might have been
// but a false response means the key was definitively not inserted
func (blm *Bloom) MayContain(key string) (bool, error) {
	exists := true
	// for each bit that needs to be set, we need the h(i) hash
	hash1, err := hashWithSeed(key, h1Seed)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrFindingKey, err)
	}
	hash2, err := hashWithSeed(key, h2Seed)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrFindingKey, err)
	}
	for i := range blm.k {
		// now generate the h(i) hash
		hash := (hash1 + uint64(i)*hash2) % uint64(blm.m)
		// get the position to check
		pos, bit := blm.getBloomFilterPos(hash)
		if blm.table[pos]&(1<<bit) == 0 {
			// this bit is not set, the key could have not been inserted
			return false, nil
		}
	}
	return exists, nil
}

// Serialize encodes a bloom filter for storing on disk and re-reading
// we're serializing so
// [4byte] - [4byte] - [4byte] - [nbyte]
// m - k - bit_lenght - bits
// the bit_length is the number of bytes the bits are e.g. a bit_length of 2 means there are
// 2 bytes of data in the bit which means the bloom filter has 16bits
func (blm *Bloom) Serialize() []byte {
	filter := make([]byte, 0)
	// encode the m - number of bits in the filter
	filter = binary.LittleEndian.AppendUint32(filter, uint32(blm.m))
	//encode the k - number of bits each key should set
	filter = binary.LittleEndian.AppendUint32(filter, uint32(blm.k))
	// encode the bit length / number of bit in our table
	filter = binary.LittleEndian.AppendUint32(filter, uint32(len(blm.table))) // each element in the array is 8bits
	// add the rest of the bit
	filter = append(filter, blm.table...)
	return filter
}

// Deserialize takes a byte of data and constructs a bloom filter from it
func Deserialize(data []byte) (*Bloom, error) {
	if len(data) < 12 {
		return nil, ErrDeserializingBloom
	}
	// first read the first 4byte - the m
	m := binary.LittleEndian.Uint32(data)
	// validate m
	if m == 0 {
		return nil, fmt.Errorf("%w: %w", ErrDeserializingBloom, ErrInvalidBitLength)
	}
	// read the k
	k := binary.LittleEndian.Uint32(data[4:])
	if k == 0 {
		return nil, fmt.Errorf("%w: %w", ErrDeserializingBloom, ErrInvalidHashNumber)
	}
	// read the bitlenght
	bl := binary.LittleEndian.Uint32(data[8:])
	if uint32(len(data)) < 12+bl {
		return nil, ErrDeserializingBloom
	}
	// now read the whole bit
	bits := data[12 : 12+bl]
	if bl*8 != m {
		// bits length is inconsistent with bit count
		return nil, ErrDeserializingBloom
	}
	return &Bloom{
		m:     int(m),
		k:     int(k),
		table: bits,
	}, nil
}

// SizeByte returns the size of the bloom filter
func (blm *Bloom) SizeByte() int {
	return len(blm.table)
}

// Table returns a copy of the bloom filter table
func (blm *Bloom) Table() []uint8 {
	cp := make([]uint8, len(blm.table))
	copy(cp, blm.table)
	return cp
}
