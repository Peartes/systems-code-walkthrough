package bloom

import "errors"

var (
	ErrHashingGeneration  = errors.New("bloom: could not create hash")
	ErrInvalidBitLength   = errors.New("bloom: invalid bloom filter bit length")
	ErrInvalidHashNumber  = errors.New("bloom: invalid bloom filter number of hash")
	ErrAddingKey          = errors.New("bloom: error adding key to bloom filter")
	ErrFindingKey         = errors.New("bloom: error finding key in bloom filter")
	ErrDeserializingBloom = errors.New("bloom: error occured deserializing bloom filter")
)
